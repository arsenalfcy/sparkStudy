package com.fu.bigdata.spark.core.framework.common

import com.fu.bigdata.spark.core.framework.controller.WordCountController
import com.fu.bigdata.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

import scala.reflect.ClassManifestFactory.Unit
import scala.reflect.ClassTag.Unit

trait TApplication {
//不传入master则默认local[*]
  def start(master:String="local[*]",app:String="app")(op: =>Unit) = {
    val sparkConf = new SparkConf().setMaster(master).setAppName(app)
    val sc = new SparkContext(sparkConf)
    EnvUtil.put(sc)

    try {
        op
    }catch {
      case ex =>println(ex.getMessage)
    }

    // TODO: 关闭连接
    EnvUtil.clear()
  }
}
