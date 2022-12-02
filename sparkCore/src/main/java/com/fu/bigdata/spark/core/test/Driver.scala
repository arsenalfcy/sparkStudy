package com.fu.bigdata.spark.core.test

import java.io.ObjectOutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    //连接服务器
    val client1 = new Socket("localhost",9999)
    val client2 = new Socket("localhost",8888)


    val out1 = client1.getOutputStream
    val objectOut1 = new ObjectOutputStream(out1)

    val task = new Task()



    objectOut1.writeObject(task)
    objectOut1.flush()
    objectOut1.close()
    client1.close()

    val subTask = new SubTask
    subTask.logic = task.logic
    subTask.datas = task.datas.takeRight(2)
    val out2 = client2.getOutputStream
    val objectOut2 = new ObjectOutputStream(out2)

    objectOut2.writeObject(task)
    objectOut2.flush()
    objectOut2.close()
    client2.close()

    println("客户端数据发送完毕 ")
  }
}
