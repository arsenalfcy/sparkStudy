package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

//早期UDAF强类型聚合函数使用DSL语法操作方法
object Spark05_SparkSQL_Test2 {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //启用hive支持enableHiveSupport()
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()
    import spark.implicits._
    spark.sql("use exer")
    //查询基本数据
    spark.sql(
      """
        |select
        |	  a.*,
        |	  p.product_name,
        |   c.area,
        |	  c.city_name
        |	from user_visit_action a
        |	join product_info p on a.click_product_id=p.product_id
        |	join city_info c on a.city_id=c.city_id
        | where a.click_product_id >-1
        |""".stripMargin).createOrReplaceTempView("t1")

    //根据区域，商品分组聚合
    spark.udf.register("cityRemark", functions.udaf(new cityRemarkUDAF()))
    spark.sql(
      """
        |select
        |	area,
        |	product_name,
        |	count(*) as clickCount,
        | cityRemark(city_name) as city_remark
        |	from
        |	t1 group by area,product_name
        |""".stripMargin).createOrReplaceTempView("t2")

    //区域内对点击数排行
    spark.sql(
      """
        |select
        |	*,
        |	rank() over(partition by area order by clickCount desc) as rank
        |	from
        |	t2
        |""".stripMargin).createOrReplaceTempView("t3")
    spark.sql(
      """
        |select
        |	*
        |	from
        |	t3 where rank <=3
        |""".stripMargin).show(false)


    spark.close()

  }

  case class Buffer(var total: Long, var cityMap: mutable.Map[String, Long])

  //自定义聚合函数：实现城市备注功能
  //1.继承aggregator，定义泛型
  class cityRemarkUDAF extends Aggregator[String, Buffer, String] {
    //缓冲区初始化
    override def zero: Buffer = {
      val map1 = mutable.Map[String,Long]()
      Buffer(0L, map1)



    }

    //更新缓冲区数据
    override def reduce(buff: Buffer, city: String): Buffer = {
      buff.total += 1
      val newCount = buff.cityMap.getOrElse(city, 0L) + 1
      buff.cityMap.update(city, newCount)
      buff
    }

    //合并缓冲区数据
    override def merge(b1: Buffer, b2: Buffer): Buffer = {
      b1.total += b2.total
      val map1 = b1.cityMap
      val map2 = b2.cityMap
      //合并两个map
      //    b1.cityMap = map1.foldLeft(map2){
      //      case (map,(city,count))=>{
      //        val newCount = map.getOrElse(city,0L) +count
      //        map.update(city,newCount)
      //        map
      //      }
      //    }

      map2.foreach {
        case (city, count) => {
          val newCount = map1.getOrElse(city, 0L) + count
          map1.update(city, newCount)
        }
      }
      b1.cityMap = map1
      b1
    }

    //将统计结果生成字符串信息
    override def finish(reduction: Buffer): String = {
      val remarkList = ListBuffer[String]()

      val totalCount = reduction.total
      val cityMap = reduction.cityMap

      //降序排列
      val cityCountList = cityMap.toList.sortWith(
        (left, right) => {
          left._2 > right._2
        }
      ).take(2)

      val hasMore = cityMap.size > 2
      var rsum = 0L
      cityCountList.foreach {
        case (city, count) => {
          val r = count * 100 / totalCount
          remarkList.append(s"${city} ${r} %")
          rsum += r
        }
      }
      if (hasMore) {
        remarkList.append(s"其他 ${100 - rsum}%")
      }
      remarkList.mkString(",")
    }

    override def bufferEncoder: Encoder[Buffer] = Encoders.product

    override def outputEncoder: Encoder[String] = Encoders.STRING
  }
}
