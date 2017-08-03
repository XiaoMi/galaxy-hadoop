package com.xiaomi.infra.galaxy.hadoop.example

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table._
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.{SDSConfiguration, SDSMapReduceUtil, SDSRecordWritable}
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}

/*
 * Copy all records from a sds table to the other sds table using spark
 */
object SparkSDSTableExample {
  def main(args: Array[String]) {
    if (args.length != 2) {
      System.err.println("Usage: SparkSDSTableExample <inputTable> <outputTable>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("Spark Codelab: SparkSDSTableExample")
    // for running directly in IDE
    sparkConf.setIfMissing("spark.master", "local[2]")
    val sc = new SparkContext(sparkConf)

    val Array(inputTable, outputTable) = args
  
    // sds scan
    val sdsProperty = new SDSTableProperty
    sdsProperty.setEndpoint("endpoint")
      .setSecretID("id")
      .setSecretKey("key")
      .setClientMaxRetry(3)

    val scanRequest = new ScanRequest().setTableName(inputTable)
      .setStartKey(null).setStopKey(null)
      .setLimit(1000).setCacheResult(false) // full table scan
    val scan = new SDSTableScan(scanRequest, sdsProperty)

    val hadoopConf = sc.hadoopConfiguration
    SDSMapReduceUtil.setSDSTableInputConf(hadoopConf, scan)

    val inputRDD = sc.newAPIHadoopRDD(hadoopConf, classOf[SDSTableInputFormat], classOf[NullWritable], classOf[SDSRecordWritable])
  
    val recordNum = inputRDD.values.map(record => 1).sum()
    System.out.println("SDS Table: " + inputTable + " record num: " + recordNum)
 
    SDSMapReduceUtil.setSDSTableOutputConf(hadoopConf, new SDSTableOutput(outputTable, sdsProperty))

    inputRDD.saveAsNewAPIHadoopFile("", classOf[NullWritable], classOf[SDSRecordWritable], classOf[SDSTableOutputFormat])

    System.out.println("Dump SDS Table: " + inputTable + " to table: " + outputTable)
  }
}