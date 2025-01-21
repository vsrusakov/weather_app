import json
import asyncio
import aiosqlite
from aiohttp import web, ClientSession


app_data = {}


async def main():
    app_data['session'] = ClientSession()

    async with app_data['session']:
        # await create_table()
        app = web.Application()
        app.add_routes([web.get('/weather', handle)])

        runner = web.AppRunner(app)
        await runner.setup()

        site = web.TCPSite(runner, 'localhost', 8000)
        await site.start()

        while True:
            await asyncio.sleep(900)


if __name__ == '__main__':
    asyncio.run(main())