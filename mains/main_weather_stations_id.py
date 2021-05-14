#!/usr/bin/env python
from accident_prediction_montreal.accidents_montreal import get_accident_df
from accident_prediction_montreal.weather import get_weather_station_id_df
from accident_prediction_montreal.utils import init_spark
from accident_prediction_montreal.preprocess import preprocess_accidents

spark = init_spark()

accident_df = preprocess_accidents(get_accident_df(spark))

get_weather_station_id_df(spark, accident_df)
