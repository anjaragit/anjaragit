from pyspark import SparkContext, SparkConf

import numpy as np
sc = SparkContext()
data = [i for i in range(10)]
distData = sc.parallelize(data)