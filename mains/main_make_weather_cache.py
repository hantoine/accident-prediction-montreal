#!/usr/bin/env python
from accident_prediction_montreal.accidents_montreal import get_accident_df
from accident_prediction_montreal.weather import get_weather_df
from accident_prediction_montreal.utils import init_spark
from accident_prediction_montreal.preprocess import preprocess_accidents

spark = init_spark()

accident_df = preprocess_accidents(get_accident_df(spark))
df = get_weather_df(spark, accident_df)
