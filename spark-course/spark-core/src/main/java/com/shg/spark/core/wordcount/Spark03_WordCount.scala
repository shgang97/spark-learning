package com.shg.spark.core.wordcount

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * @author: shg
 * @create: 2022-05-21 1:42 上午
 */
object Spark03_WordCount {

  def main(args: Array[String]): Unit = {

    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    val lines: RDD[String] = sparkContext.textFile("data")
    val words: RDD[String] = lines.flatMap(line => {
      line.split(" ")
    })
    val wordToOne: RDD[(String, Int)] = words.map(
      word => (word, 1)
    )
//    val wordToCount = wordToOne.reduceByKey((x, y) => {x + y})
//    val wordToCount = wordToOne.reduceByKey((x, y) => x + y)
    val wordToCount = wordToOne.reduceByKey(_ + _)

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sparkContext.stop()
  }

}
