package com.shg.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: shg
 * @create: 2022-05-21 1:23 上午
 */
object Spark02_WordCount {
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
    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(t => t._1)
    val wordToCount = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sparkContext.stop()
  }

}
