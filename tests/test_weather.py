from accident_prediction_montreal.weather import get_weather_station_id

def test_get_weather_station_id():
    station_ids = get_weather_station_id(
        lat=45.48381,
        long=-73.57959,
        year=2012,
        month=4,
        day=13
    )
    assert station_ids == [10761, 30165, 5490, 48374, 5415, 5389, 5484, 5313, 5441]
