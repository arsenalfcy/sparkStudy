package com.fu.bigdata.spark.core.framework.controller

import com.fu.bigdata.spark.core.framework.common.TController
import com.fu.bigdata.spark.core.framework.service.WordCountService


class WordCountController extends TController{

  private val wordCountService = new WordCountService()

  //调度
  def dispatch()={

    val tuples = wordCountService.dataAnalysis()
    tuples.foreach(println)
  }
}
