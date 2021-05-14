#!/usr/bin/env python
from accident_prediction_montreal.preprocess import get_positive_samples
from accident_prediction_montreal.utils import init_spark
spark = init_spark()
pos_samples = get_positive_samples(spark)
print(pos_samples.count())
