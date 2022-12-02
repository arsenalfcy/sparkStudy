package com.fu.bigdata.spark.core.framework.util

import org.apache.spark.SparkContext

object EnvUtil {

  //在线程中使用一块内存存放sc，方便dao层调用
  private val scLocal = new ThreadLocal[SparkContext]()

  def put(sc:SparkContext)={
    scLocal.set(sc)
  }

  def take()={
    scLocal.get()
  }

  def clear()={
    scLocal.remove()
  }
}
