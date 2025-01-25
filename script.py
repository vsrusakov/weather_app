from db_queries import db_queries
from array import array
import asyncio
import aiosqlite
from aiohttp import web, ClientSession
from aiohttp.web_request import Request
from aiohttp.web_response import Response
from multidict._multidict import MultiDictProxy


app_data = {}
TIME_ZONE = 'Europe/Moscow'
RETURN_VALUES = ('precipitation', 'temperature', 'wind_speed', 'humidity')


def valudate_time(query: MultiDictProxy) -> int:
    """
    Проверка времени на соответствие формату hh:mm.
    """
    time = query['time'].split(':')
    assert len(time) == 2, f'Invalid time format. Expected hh:mm. Got {query["time"]}'
    
    hh, mm = map(int, time)
    assert 0 <= hh <= 23, f'Invalid hour value {hh}'
    assert 0 <= mm <= 59, f'Invalid minutes value {mm}'
    return hh


def valudate_city(query: MultiDictProxy) -> str:
    """
    Проверка того, что передана непустая строка.
    """
    assert len(query['city']) > 0, 'Invalid city name'
    return query['city'].lower()


def validate_ret_values(query: MultiDictProxy) -> list[str]:
    """
    Функция валидирует значения параметра return в get-запросе к /weather (метод №4).
    """
    ret_values = query.get('return', 'temperature').split(',')
    assert all(p in RETURN_VALUES for p in ret_values), f'Invalid return values(s): {", ".join(query["return"])}'
    return ret_values


def process_forecast(
        data_json: dict
    ) -> tuple[float, bytes, bytes, bytes]:
    """
    Функция обрабатывает json-данные о погоде, полученные от open meteo, извлекая те значения,
    которые нужны для внесения в таблицу city_forecasts.
    """
    precip = data_json['daily']['precipitation_sum'][0]
    hourly = data_json['hourly']
    temp = array('f', hourly['temperature_2m']).tobytes()
    wind = array('f', hourly['wind_speed_10m']).tobytes()
    humidity = bytes(hourly['relative_humidity_2m'])
    return precip, temp, wind, humidity


async def db_city_forecast(city: str, ret_values: list[str]) -> aiosqlite.Row:
    """
    Сопрограмма извлекает из БД данные о погоде для города city. В аргумент ret_values
    передаются имена нужных столбцов.
    """
    query = db_queries['city_row'].format(', '.join(ret_values))

    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (city,)) as cursor:
            return await cursor.fetchone()


async def initialize_db() -> None:
    """
    Сопрограмма создает таблицу city_forecasts и индекс по полю city на основе этой таблицы.
    """
    query_create = db_queries['create_city_forecasts']
    query_index = db_queries['index_city_forecasts']
    async with aiosqlite.connect('weather.db') as db:
        await db.execute(query_create)
        await db.execute(query_index)
        await db.commit()


async def save_to_db(table_name: str, values: tuple) -> None:
    """
    Сопрограмма для вставки в таблицу table_name значений values.
    """
    query = db_queries[f'insert_{table_name}']
    async with aiosqlite.connect('weather.db') as db:
        await db.execute(query, values) 
        await db.commit()


async def db_find_city(city: str) -> int:
    """
    Сопрограмма для проверки наличия в таблице city_forecasts строки, в которой
    поле city равно аргументу city. Если строка найдена, возвращает 
    ее ROWID, иначе возвращает -1.
    """
    async with aiosqlite.connect('weather.db') as db:
        async with db.execute(db_queries['find_city'], (city,)) as cursor:
            row_id = await cursor.fetchone()
            return row_id if row_id else -1


async def update_forecasts() -> None:
    """
    Сопрограмма для обновления данных о прогнозе погоды в городах из
    таблицы city_forecasts.
    """
    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(db_queries['select_rows']) as cursor:
            async for row_id, lat, lon, *fc in cursor:
                params = {
                    'latitude': lat,
                    'longitude': lon,
                    'timezone': TIME_ZONE,
                    'hourly': ['temperature_2m', 'wind_speed_10m', 'relative_humidity_2m'],
                    'daily': 'precipitation_sum',
                    'forecast_days': 1,
                }
                data, status = await open_meteo_api(params)
                if status == 200:
                    new_fc = process_forecast(data)
                    for i, value in enumerate(new_fc):
                        if value != fc[i]:
                            query = db_queries['update_forecasts'].format(RETURN_VALUES[i])
                            await db.execute(query, (value, row_id))


async def weather_by_coords(query: MultiDictProxy) -> tuple[dict, int]:
    """
    Сопрограмма для получения данных о погоде на текущий момент по координатам.
    """
    params = {
        'latitude': query['lat'],
        'longitude': query['lon'],
        'timezone': TIME_ZONE,
        'current': ['temperature_2m', 'wind_speed_10m', 'pressure_msl'],
    }
    data, status = await open_meteo_api(params)
    if status == 200:
        new_data = {}
        new_data['lat'] = query['lat']
        new_data['lon'] = query['lon']
        new_data['temperature'] = data['current']['temperature_2m']
        new_data['wind_speed'] = data['current']['wind_speed_10m']
        new_data['pressure'] = data['current']['pressure_msl']
        data = new_data
    return data, status


async def weather_for_city(query: MultiDictProxy) -> tuple[dict, int]:
    """
    Сопрограмма для получения данных о погоде по названию города и времени.
    """
    try:
        city = valudate_city(query)
        hour = valudate_time(query)
        ret_values = validate_ret_values(query)
    except AssertionError as e:
        data = {'error': True, 'reason': str(e)}
        status = 400
    except ValueError:
        data = {'error': True, 'reason': 'Invalid hour or minute values'}
        status = 400
    except Exception as e:
        data = {'error': True, 'reason': 'Server error'}
        status = 500
    else:
        row = await db_city_forecast(city, ret_values)

        if row is None:
            data = {'error': True, 'reason': 'The city is not in the database'}
            status = 400
            return data, status

        row = dict(row)
        for param in ret_values:
            if param == 'precipitation':
                continue
            row[param] = array('f', row[param])[hour]
        data = row
        status = 200
    
    return data, status


async def open_meteo_api(params: dict) -> tuple[dict, int]:
    """
    Сопрограмма для отправки запросов к API open meteo.
    """
    url = 'https://api.open-meteo.com/v1/forecast'
    async with app_data['session'].get(url=url, params=params) as response:
        return await response.json(), response.status


async def get_weather(request: Request) -> Response:
    """
    Обработчик get-запросов к ресурсу /weather . Реализует методы №1 и №4.
    """
    data = None
    status = 200
    query = request.query

    if 'lat' in query and 'lon' in query:
        data, status = await weather_by_coords(query)
    elif 'city' in query and 'time' in query:
        data, status = await weather_for_city(query)
    else:
        data = {'error': True, 'reason': f'Invalid query parameters: {", ".join(query)}'}
        status = 400
    
    return web.json_response(data=data, status=status)


async def get_cities(request: Request) -> Response:
    """
    Обработчик get-запросов к ресурсу /cities . Реализует метод №3.
    """
    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(db_queries['select_cities']) as cursor:
            cities = [dict(row) async for row in cursor]
    return web.json_response(data=cities)


async def post_city(request: Request) -> Response:
    """
    Обработчик post-запросов к ресурсу /city . Реализует метод №2.
    """
    data = {'result': 'City saved'}
    status = 200
    body = await request.post()

    city = body.get('city', '').lower()
    if not city:
        data = {'error': True, 'reason': 'The city parameter is not passed'}
        status = 400
        return web.json_response(data=data, status=status)
    
    city_id = await db_find_city(city)
    if city_id == -1:
        lat, lon = body.get('lat', ''), body.get('lon', '')
        params = {
            'latitude': lat,
            'longitude': lon,
            'timezone': TIME_ZONE,
            'hourly': ['temperature_2m', 'wind_speed_10m', 'relative_humidity_2m'],
            'daily': 'precipitation_sum',
            'forecast_days': 1,
        }
        data, status = await open_meteo_api(params)

        if status == 200:
            forecast = process_forecast(data)
            row_values = (city.lower(), lat, lon, *forecast)
            await save_to_db('city_forecasts', row_values)
            data = {'result': 'City saved'}
    
    return web.json_response(data=data, status=status)


async def main() -> None:
    app_data['session'] = ClientSession()

    async with app_data['session']:
        await initialize_db()

        app = web.Application()
        app.add_routes([
            web.get('/weather', get_weather),
            web.get('/cities', get_cities),
            web.post('/city', post_city),
        ])

        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, 'localhost', 8000)
        await site.start()

        while True:
            await asyncio.sleep(900)
            await update_forecasts()


if __name__ == '__main__':
    asyncio.run(main())
