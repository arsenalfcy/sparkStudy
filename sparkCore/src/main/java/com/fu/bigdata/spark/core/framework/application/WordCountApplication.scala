package com.fu.bigdata.spark.core.framework.application

import com.fu.bigdata.spark.core.framework.common.TApplication
import com.fu.bigdata.spark.core.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

//TODO：按照工程化结构代码写的wordcount
object WordCountApplication extends App with TApplication{

  //启动应用
  start(){
    val controller = new WordCountController()
    controller.dispatch()
  }



}
