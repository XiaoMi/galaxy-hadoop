package com.xiaomi.infra.galaxy.hadoop

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Copyright 2017, Xiaomi.
  * All rights reserved.
  * Author: liupengcheng@xiaomi.com
  */
object FdsScan {

  def main(args: Array[String]): Unit = {
    if (args.length != 2) {
      println("Usage: $0 <input> <output>")
      println("Descriptions:\n fds uri format is like fds://ak:sk@bucket.region/path")
      throw new IllegalArgumentException("Only accept two arguments!")
    }

    val inputUri = args(0)
    val outputUri = args(1)

    val sparkConf = new SparkConf().setAppName("Spark Codelab: Spark FDS scan")
    sparkConf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(sparkConf)

    val data = sc.textFile(inputUri)
    val result = data.flatMap(x => x.split(" ")).map(x => (x, 1)).reduceByKey(_ + _)
    result.saveAsTextFile(outputUri)
  }

}
