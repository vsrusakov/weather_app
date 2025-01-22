import json
# import db_queries
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


async def open_meteo_api(params):
    url = 'https://api.open-meteo.com/v1/forecast'
    async with app_data['session'].get(url=url, params=params) as response:
        return await response.json(), response.status


async def get_weather(request):
    data_json = None
    status = 200
    q = request.query

    if 'lat' in q and 'lon' in q:
        params = {
            'latitude': q.get('lat'),
            'longitude': q.get('lon'),
            'timezone': q.get('timezone', DEF_TIME_ZONE),
            'current': ['temperature_2m', 'wind_speed_10m', 'pressure_msl'],
        }
        data_json, status = await open_meteo_api(params)
    elif 'name' in q and 'time' in q:
        pass
    else:
        pass
    
    if status == 200:
        del data_json['generationtime_ms'], data_json['elevation']
    
    return web.json_response(data=data_json, status=status)


async def get_cities(request):
    pass


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

        row_values = (city, lat, lon, b_forecats)
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
            await update_forecasts()
            await asyncio.sleep(900)
            # await update_forecasts()


if __name__ == '__main__':
    asyncio.run(main())