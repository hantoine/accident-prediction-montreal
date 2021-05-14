#!/usr/bin/env python
from accident_prediction_montreal.preprocess import get_negative_samples, get_positive_samples
from accident_prediction_montreal.utils import init_spark
from accident_prediction_montreal.workdir import workdir
spark = init_spark()
neg_samples = \
    get_negative_samples(spark,
                         save_to='data/negative-sample-new.parquet',
                         sample_ratio=1e-3)
print(neg_samples.count())
