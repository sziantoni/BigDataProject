from __future__ import print_function

import sys

from pyspark import SparkContext

import time
from io import StringIO
import pandas
import s3fs


if __name__ == "__main__":
       data = []
       sc = SparkContext()
       file_ = sc.textFile("s3a://[MY BUCKET]/dump.txt")
       counts = file_.map(lambda line: [(i, 1) for i in set(line.split(" "))]).flatMap(lambda x: x).reduceByKey(lambda x, y: x + y).collect()
       for i in range(0, 1000):
           print(counts[i])

       #df = pandas.DataFrame(counts, columns=['word', 'count'])

       #btw = df.to_csv(index = False).encode()

       #fs = s3fs.S3FileSystem(key='[AWS KEY ID], secret='[AWS SECRET KEY]')

       #with fs.open('s3://resultsziantoni/file_test.csv', 'wb') as f:
       #       f.write(btw)

       sc.stop()
