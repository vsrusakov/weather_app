import json
# import db_queries
from datetime import date
from db_queries import db_queries
import asyncio
import aiosqlite
from aiohttp import web, ClientSession


app_data = {}
DEF_TIME_ZONE = 'Europe/Moscow'


def bin_forecast(data_json):
    forecats = data_json['hourly']
    forecats['hourly_units'] = data_json['hourly_units']
    return json.dumps(forecats, ensure_ascii=False).encode('utf-8')


async def city_coords(city):
    async with aiosqlite.connect('weather.db') as db:
        async with db.execute(db_queries['city_coords'], (city,)) as cursor:
            return await cursor.fetchone()


async def execute_query(query_name):
    query = db_queries[query_name]
    async with aiosqlite.connect('weather.db') as db:
        await db.execute(query) 
        await db.commit()


async def save_to_db(table_name, values):
    query = db_queries[f'insert_{table_name}']
    async with aiosqlite.connect('weather.db') as db:
        await db.execute(query, values) 
        await db.commit()


async def update_forecasts():
    async with aiosqlite.connect('weather.db') as db:
        async with db.execute(db_queries['select_coords']) as cursor:
            async for lat, lon, fc, row_id in cursor:
                params = {
                    'latitude': lat,
                    'longitude': lon,
                    'timezone': DEF_TIME_ZONE,
                    'hourly': ['temperature_2m', 'wind_speed_10m'],
                    'forecast_days': 1,
                }
                data_json, status = await open_meteo_api(params)
                if status == 200:
                    new_forecast = bin_forecast(data_json)
                    if new_forecast != fc:
                        await db.execute(db_queries['update_forecasts'], (row_id, new_forecast))
                        await db.commit()


async def weather_by_coords(query):
    params = {
        'latitude': query['lat'],
        'longitude': query['lon'],
        'timezone': query.get('timezone', DEF_TIME_ZONE),
        'current': ['temperature_2m', 'wind_speed_10m', 'pressure_msl'],
    }
    data, status = await open_meteo_api(params)
    if status == 200:
        del data['generationtime_ms'], data['elevation']
    return data, status


async def weather_for_city(query):

    coords = await city_coords(query['city'])

    if coords is None:
        data = {'error': True, 'reason': 'The city is not in the database'}
        status = 400
    else:
        ret_params = query.get('return', 'temperature_2m').split(',')
        time = f'{date.today().isoformat()}T{query["time"]}'
        params = {
            'latitude': coords[0],
            'longitude': coords[1],
            'timezone': DEF_TIME_ZONE,
            'start_hour': time,
            'end_hour': time,
        }
        if 'precipitation' in ret_params:
            params.update({
                'daily': 'precipitation_sum',
                'start_date': date.today().isoformat(),
                'end_date': date.today().isoformat(),
            })
            ret_params.remove('precipitation')
        params['hourly'] = ret_params

        data, status = await open_meteo_api(params)

        if status == 200:
            data = data.get('hourly', {}) | data.get('daily', {})
            del data['time']
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
    data_json = None
    status = 200
    q = await request.post()

    lat, lon, city = q.get('lat'), q.get('lon'), q.get('city')
    params = {
        'latitude': lat,
        'longitude': lon,
        'timezone': q.get('timezone', DEF_TIME_ZONE),
        'hourly': ['temperature_2m', 'wind_speed_10m'],
        'forecast_days': 1,
    }
    data_json, status = await open_meteo_api(params)

    if status == 200:
        b_forecats = bin_forecast(data_json)

        row_values = (city.lower(), lat, lon, b_forecats)
        await save_to_db('city_forecasts', row_values)
        data_json = {'result': 'City saved'}
    
    return web.json_response(data=data_json, status=status)


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
            await asyncio.sleep(900)
            await update_forecasts()


if __name__ == '__main__':
    asyncio.run(main())