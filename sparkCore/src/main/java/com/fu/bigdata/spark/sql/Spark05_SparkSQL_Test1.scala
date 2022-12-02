package com.fu.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._

//早期UDAF强类型聚合函数使用DSL语法操作方法
object Spark05_SparkSQL_Test1 {
  def main(args: Array[String]): Unit = {

    // TODO: 创建sparksql运行环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
    //启用hive支持enableHiveSupport()
    val spark = SparkSession.builder().enableHiveSupport().config(sparkConf).getOrCreate()

    spark.sql("use exer")
    //准备数据
    spark.sql(
      """
        |select
        |	*
        |from (
        |	select
        |	* ,
        |	rank() over (partition by area order by clickCount desc) as rank
        |from(
        |	select
        |	area,
        |	product_name,
        |	count(*) as clickCount
        |	from(
        |		select
        |	a.*,
        |	p.product_name,
        |	c.area,
        |	c.city_name
        |from user_visit_action a
        |join product_info p on a.click_product_id=p.product_id
        |join city_info c on a.city_id=c.city_id
        |where a.click_product_id >-1
        |		) t1 group by area,product_name
        |)t2
        |)t3 where rank <=3
        |""".stripMargin).show()
    spark.close()

  }


}
