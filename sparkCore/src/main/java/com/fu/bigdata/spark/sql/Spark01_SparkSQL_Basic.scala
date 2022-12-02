package com.fu.bigdata.spark.sql


import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}


object sSpark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    import  spark.implicits._


    // TODO: 执行逻辑操作


    //DataFrame
    val df = spark.read.json("datas/user.json")
//    df.show()

    //DataFrame =>SQL
    df.createOrReplaceTempView("user")

    spark.sql("select * from user").show()
    spark.sql("select avg(age) from user").show()

    //DataFrame=>DSL
    //使用dataframe时，如果涉及到转换操作，需要引入转换规则
    df.select('age+1).show()

    //DataSet
    val seq =Seq(1,2,3,4)
    val ds =seq.toDS()
    ds.show()





    //RDD=>DataFrame
    val rdd =spark.sparkContext.makeRDD(List((1,"zhangsan",30),(2,"lisi",40)))
    val df1: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD = df1.rdd



    //DataFrame=>DataSet
    val ds1 = df1.as[User]
    val df2: DataFrame = ds1.toDF()

    //RDD=>DataSet
    val ds2 = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()

    val userRDD: RDD[User] = ds2.rdd


    // TODO: 关闭环境
    spark.close()
  }

  case class User(id:Int,name:String,age:Int)

}
