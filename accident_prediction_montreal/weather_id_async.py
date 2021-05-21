import asyncio
import aiohttp
import backoff
from bs4 import BeautifulSoup


def get_weather_station_ids(accidents_infos):
    loop = asyncio.get_event_loop()
    try:
        return loop.run_until_complete(get_weather_station_ids_async(accidents_infos))
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()


async def get_weather_station_ids_async(accidents_infos):
    async with aiohttp.ClientSession() as client:
        futures = [get_weather_station_id_async(client, **acc_info.asDict())
                   for acc_info in accidents_infos]
        stations_ids = set()
        for future in asyncio.as_completed(futures):
            stations_ids.update(await future)
        return stations_ids


@backoff.on_exception(backoff.expo, aiohttp.ClientError, max_tries=10, max_time=60)
async def get_weather_station_id_async(client, lat, long, year, month, day):
    """Get data from all stations."""
    lat = degree_to_DMS(lat)
    long = degree_to_DMS(long)
    url = (
        f"https://climate.weather.gc.ca/historical_data/"
        f"search_historic_data_stations_e.html?searchType=stnProx&"
        f"timeframe=1&txtRadius=25&selCity=&selPark=&optProxType=custom&"
        f"txtCentralLatDeg={abs(lat[0])}&txtCentralLatMin={lat[1]}&"
        f"txtCentralLatSec={lat[2]:.1f}&txtCentralLongDeg={abs(long[0])}&"
        f"txtCentralLongMin={long[1]}&txtCentralLongSec={long[2]:.1f}&"
        f"txtLatDecDeg=&txtLongDecDeg=&"
        f"StartYear=1840&EndYear=2019&optLimit=specDate&Year={year}&"
        f"Month={month}&Day={day}&selRowPerPage=100"
    )
    async with client.get(url, raise_for_status=True) as response:
        page = BeautifulSoup(await response.text(), "lxml")
        stations = page.body.main.find(
            "div", class_="historical-data-results proximity hidden-lg"
        ).find_all("form", recursive=False)
        return [int(s.find("input", {"name": "StationID"})["value"])
                for s in stations]


def degree_to_DMS(degree):
    """Convert from plain degrees format to DMS format of geolocalization.
    DMS: "Degrees, Minutes, Seconds" is a format for coordinates at the surface
        of earth. Decimal Degrees = degrees + (minutes/60) + (seconds/3600)
        This measure permit to gain in precision when a degree is not precise
        enough.
    """
    return (
        int(degree),
        int(60 * (abs(degree) % 1)),
        ((60 * (abs(degree) % 1)) % 1) * 60,
    )