package com.fu.bigdata.spark.core.framework.service

import com.fu.bigdata.spark.core.framework.common.TService
import com.fu.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

class WordCountService extends TService {

  private val wordCountDao = new WordCountDao()

  //数据分析
  def dataAnalysis() = {
    // TODO: 执行业务操作


    val lines = wordCountDao.readFile("datas/word.txt")
    //2.将一行数据进行拆分，形成一个一个的单词
    val words = lines.flatMap(_.split(" "))

    val wordToOne = words.map(word => (word, 1))
    //3.将数据根据单词进行分组，便于统计
    val groupedWords = wordToOne.groupBy(word => word._1)

    //4.对分组后的数据进行转换
    val result: RDD[(String, Int)] = groupedWords.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => {
            (t1._1, t1._2 + t2._2)
          }
        )
      }
    }

    //5.将转换结果采集到控制台打印
    val tuples = result.collect()
    tuples
  }
}
