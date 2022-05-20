package com.shg.spark.core.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author: shg
 * @create: 2022-05-21 12:46 上午
 */
object Spark01_WordCount {
  def main(args: Array[String]): Unit = {

    // Application
    // Spark框架

    // TODO 建立和Spark框架的连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sparkContext = new SparkContext(sparkConf)

    // TODO 执行业务操作
    // 1. 读取文件，获取一行一行的数据
    val lines: RDD[String] = sparkContext.textFile("data")
    // 2. 将一行数据进行拆分，形成一个一个单词
    // 扁平化：将整体拆分成个体的操作
    val words: RDD[String] = lines.flatMap(line => {
      line.split(" ")
    })
    // 3. 将数据根据单词进行分组，便于统计
    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    // 4. 对分组后的数据进行转换
    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }
    // 5. 将转换结果采集到控制台打印出来
    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)

    // TODO 关闭连接
    sparkContext.stop()
  }

}
