package com.fu.bigdata.spark.core.test

import java.io.ObjectInputStream
import java.net.ServerSocket

object Executor {
  def main(args: Array[String]): Unit = {

    //启动服务器，接受数据
    val server = new ServerSocket(9999)
    println("服务器启动，等待接受数据")

    //等待客户端连接
    val client = server.accept()

    val in = client.getInputStream
    val objectIn = new ObjectInputStream(in)
    val task = objectIn.readObject().asInstanceOf[Task]
    val ints = task.compute()

    println("计算结果为："+ ints)
    objectIn.close()
    client.close()

    server.close()
  }
}
