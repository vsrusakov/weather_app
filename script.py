import json
from datetime import date
from db_queries import db_queries
from array import array
import asyncio
import aiosqlite
from aiohttp import web, ClientSession


app_data = {}
DEF_TIME_ZONE = 'Europe/Moscow'
RETURN_PARAMS = ('precipitation', 'temperature', 'wind_speed', 'humidity')


def valudate_time(query):
    time = query['time'].split(':')
    assert len(time) == 2, f'Invalid time format. Expected hh:mm. Got {query["time"]}'
    
    hh, mm = map(int, time)
    assert 0 <= hh <= 23, f'Invalid hour value {hh}'
    assert 0 <= mm <= 59, f'Invalid minutes value {mm}'
    return hh


def valudate_city(query):
    assert len(query['city']) > 0, 'Invalid city name'
    return query['city'].lower()


def validate_ret_params(query):
    ret_params = query.get('return', 'temperature').split(',')
    assert all(p in RETURN_PARAMS for p in ret_params), f'Invalid parameter(s): {", ".join(query["return"])}'
    return ret_params


def process_forecast(data_json):
    precip = data_json['daily']['precipitation_sum'][0]
    hourly = data_json['hourly']
    temp = array('f', hourly['temperature_2m']).tobytes()
    wind = array('f', hourly['wind_speed_10m']).tobytes()
    humidity = bytes(hourly['relative_humidity_2m'])
    return precip, temp, wind, humidity


async def city_row(city, ret_params):
    query = db_queries['city_row'].format(', '.join(ret_params))

    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, (city,)) as cursor:
            return await cursor.fetchone()


async def execute_query(query_name):
    query = db_queries[query_name]
    async with aiosqlite.connect('weather.db') as db:
        await db.execute(query) 
        await db.commit()


async def save_to_db(table_name, values):
    query = db_queries[f'insert_{table_name}']
    try:
        async with aiosqlite.connect('weather.db') as db:
            await db.execute(query, values) 
            await db.commit()
    except aiosqlite.IntegrityError:
        pass

async def update_forecasts():
    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(db_queries['select_rows']) as cursor:
            async for row_id, lat, lon, *fc in cursor:
                params = {
                    'latitude': lat,
                    'longitude': lon,
                    'timezone': DEF_TIME_ZONE,
                    'hourly': ['temperature_2m', 'wind_speed_10m', 'relative_humidity_2m'],
                    'daily': 'precipitation_sum',
                    'forecast_days': 1,
                }
                data, status = await open_meteo_api(params)
                if status == 200:
                    new_fc = process_forecast(data)
                    for i, value in enumerate(new_fc):
                        if value != fc[i]:
                            query = db_queries['update_forecasts'].format(RETURN_PARAMS[i])
                            await db.execute(query, (value, row_id))


async def weather_by_coords(query):
    params = {
        'latitude': query['lat'],
        'longitude': query['lon'],
        'timezone': DEF_TIME_ZONE,
        'current': ['temperature_2m', 'wind_speed_10m', 'pressure_msl'],
    }
    data, status = await open_meteo_api(params)
    if status == 200:
        del data['generationtime_ms'], data['elevation']
    return data, status


async def weather_for_city(query):

    try:
        city = valudate_city(query)
        hour = valudate_time(query)
        ret_params = validate_ret_params(query)
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
        row = await city_row(city, ret_params)

        if row is None:
            data = {'error': True, 'reason': 'The city is not in the database'}
            status = 400
            return data, status

        row = dict(row)
        for param in ret_params:
            if param == 'precipitation':
                continue
            row[param] = array('f', row[param])[hour]
        data = row
        status = 200
    
    return data, status


async def open_meteo_api(params):
    url = 'https://api.open-meteo.com/v1/forecast'
    async with app_data['session'].get(url=url, params=params) as response:
        return await response.json(), response.status


async def get_weather(request):
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


async def get_cities(request):
    async with aiosqlite.connect('weather.db') as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(db_queries['select_cities']) as cursor:
            cities = [dict(row) async for row in cursor]
    return web.json_response(data=cities)


async def post_city(request):
    data = None
    status = 200
    body = await request.post()

    lat, lon, city = body.get('lat', ''), body.get('lon', ''), body.get('city')
    if not city:
        data = {'error': True, 'reason': 'The city parameter is not passed'}
        status = 400
        return web.json_response(data=data, status=status)
    
    params = {
        'latitude': lat,
        'longitude': lon,
        'timezone': body.get('timezone', DEF_TIME_ZONE),
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


async def main():
    app_data['session'] = ClientSession()

    async with app_data['session']:
        await execute_query('create_city_forecasts')

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
            await asyncio.sleep(60)
            await update_forecasts()


if __name__ == '__main__':
    asyncio.run(main())