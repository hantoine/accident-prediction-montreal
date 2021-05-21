import asyncio
import aiohttp
from accident_prediction_montreal.weather_id_async import get_weather_station_id_async


def test_get_weather_station_id_async():
    loop = asyncio.get_event_loop()
    try:
        station_ids = loop.run_until_complete(get_weather_station_ids_async_helper(
            lat=45.48381,
            long=-73.57959,
            year=2012,
            month=4,
            day=13,
        ))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
    assert station_ids == [10761, 30165, 5490, 48374, 5415, 5389, 5484, 5313, 5441]


async def get_weather_station_ids_async_helper(**kargs):
    async with aiohttp.ClientSession() as client:
        return await get_weather_station_id_async(client, **kargs)
