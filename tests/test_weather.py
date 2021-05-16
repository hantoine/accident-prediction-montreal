import pytest
import shutil

from accident_prediction_montreal.weather import (
    get_weather_station_id,
    get_weather_station_id_df,
)
from accident_prediction_montreal.utils import init_spark


@pytest.fixture(scope='session')
def spark():
    return init_spark()


def test_get_weather_station_id():
    station_ids = get_weather_station_id(
        lat=45.48381,
        long=-73.57959,
        year=2012,
        month=4,
        day=13
    )
    assert station_ids == [10761, 30165, 5490, 48374, 5415, 5389, 5484, 5313, 5441]


def test_get_weather_station_id_df(spark):
    acc_sample = spark.read.parquet('tests/data/preprocessed_accidents_sample.parquet')
    acc_sample = acc_sample.limit(15)
    get_weather_station_id_df(spark, acc_sample, cache_file='/tmp/test.parquet')
    shutil.rmtree('/tmp/test.parquet')
