package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @Author: Sdaer
 * @Date: 2020-08-18
 * @Desc: GMV需求实时计算
 *        传输数据，进行计算保存
 *
 *        程序执行流程
 *        1）创建SparkConf配置
 *        2）根据SparkConf创建Sparkstreaming
 *        3）从Kafka工具类获取主题消费流
 *        4）从流中获取每一批数据转换成样例类，并添加两个时间字段
 *        5)	给手机号脱敏
 *        6）将数据写入HBase(Phoenix)
 *
 *        注意点
 *        1）为什么使用canal
 *            canal是一个实时监控数据库变化的框架，在数据库中数据被改变后，canal可以监控到
 *            并可以实时传输这些数据进行解析或远程同步数据库
 *            canal外部套了一层从数据库的壳，因此可以实时获取数据
 *        2）数据保存到Phoenix的隐式方法
 *            将数据保存到Phoenix需要借助org.apache.phoenix.spark._
 *            本包提供了对rdd的隐式扩展
 */
object OrderApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf以及StreamingContext
    val sparkConf = new SparkConf().setAppName("order_app").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka order_info主题数据创建流
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)

    //3.将每行数据转换为样例类：给日期及小时字段重新赋值，给联系人手机号脱敏
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(record => {

      //a.转化为样例类对象
      val orderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      //b.给日期及小时字段重新赋值
      val create_time = orderInfo.create_time //2020-08-18 04:24:04
      val timeArr = create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)

      //c.给手机号脱敏
      orderInfo.consignee_tel = orderInfo.consignee_tel.substring(0, 2) + "****" + orderInfo.consignee_tel.substring(8, 10)

      //d.返回数据
      orderInfo

    })

    //测试输出
    orderInfoDStream.cache()
    orderInfoDStream.print()

    //4.将数据写入phoenix
    orderInfoDStream.foreachRDD(rdd => {

      rdd.saveToPhoenix(
        "GMALL200317_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181")
      )

    })

    //5.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
