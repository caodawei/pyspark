from pyspark.conf import SparkConf
from pyspark.context import SparkContext


def fun1():
    pass


if __name__ == '__main__':
    conf = SparkConf()
    conf.setMaster("local").setAppName("wc")
    sc = SparkContext(conf=conf)
    rdd = sc.textFile("words.txt")
    rdd = rdd.flatMap(lambda s: s.split(" "))
    rdd = rdd.map(lambda s: (s, 1))
    rdd = rdd.reduceByKey(lambda v1, v2: v1 + v2)
    rdd.foreach(lambda s: print(s))
