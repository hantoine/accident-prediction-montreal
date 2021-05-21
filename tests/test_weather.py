import pytest
import shutil

from accident_prediction_montreal.weather import get_weather_station_id_df
from accident_prediction_montreal.utils import init_spark


@pytest.fixture(scope='session')
def spark():
    return init_spark()


def test_get_weather_station_id_df(spark):
    acc_sample = spark.read.parquet('tests/data/preprocessed_accidents_sample.parquet')
    acc_sample = acc_sample.limit(200)
    get_weather_station_id_df(spark, acc_sample, cache_file='/tmp/test.parquet')
    shutil.rmtree('/tmp/test.parquet')
