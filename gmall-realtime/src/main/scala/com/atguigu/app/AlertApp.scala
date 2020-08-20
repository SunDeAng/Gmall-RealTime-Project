package com.atguigu.app

import java.text.SimpleDateFormat
import java.time.LocalDate
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{CouponAlertInfo, EventLog}
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
 * @Author: Sdaer
 * @Date: 2020-08-18
 * @Desc:
 *       预警执行流程
 *       1.创建SparkConf和StreamingContext
 *       2.读取Kafka事件主题数据创建流
 *       3.转化为样例类对象
 *       4.开5分钟的窗口
 *       5.按照mid做分组处理
 *       6.对单条数据进行处理
 *          6.1三次及以上用不同账号(登陆并领取优惠券)：对uid进行去重
 *          6.2没有浏览商品：反面考虑，如果有浏览商品，当前mid不产生预警日志
 *              a.创建Set用于存放领券的UID；
 *                创建用于存放领券涉及的商品ID；
 *                创建List用于存放用户行为
 *                定义标志位用于标识是否有浏览行为
 *              b.遍历logIter
 *                提取时间类型（防止代码冗余）
 *                判断当前数据是否有领券行为并执行相应的操作
 *       7.将生成的预警日志写入ES
 *
 *       注意点
 *       1）为什么要写入ES
 *          ES支持分词查询，这是ES最大的优势，同时ES对接了Kibana页面显示框架，可以实时的显示结果
 *       2）需求的解决思路是什么
 *          (1)开5分钟的窗口:对应需求的5分组，其次是5秒的步长，如果不配，默认是批次的时间间隔
 *          (2)按照mid分组：必须是一设备多账号，故按照mid分组
 *          (3)三次不同账户：对分组后的uid进行去重
 *          (4)没有浏览商品：就是有浏览商品的行为就不产生预警日志
 *          (5)每分钟记录一次：在ES存储中，我们可以把ID设置为分钟时间戳，这样可以对每分钟去重
 */
object AlertApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkConf和StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.读取Kafka事件主题数据创建流
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_EVENT, ssc)

    //3.转化为样例类对象
    val eventLogDStream = kafkaDStream.map(record => {
      //a.转换
      val eventLog: EventLog = JSON.parseObject(record.value(), classOf[EventLog])

      //b.处理日志日期和小时
      val dateHour: String = sdf.format(new Date(eventLog.ts))
      val dateHourArr = dateHour.split(" ")
      eventLog.logDate = dateHourArr(0)
      eventLog.logHour = dateHourArr(1)

      //c.返回结果
      eventLog
    })

    //打印测试
//    println("*********第三步骤测试*********")
//    eventLogDStream.print()

    //4.开5分钟的窗口
    val windowDStream = eventLogDStream.window(Minutes(5))

    //5.按照mid做分组处理
    val midToEventLogIterDStream: DStream[(String, Iterable[EventLog])] = windowDStream.map(eventLog => (eventLog.mid, eventLog)).groupByKey()

    //6.对单条数据进行处理
    //6.1三次及以上用不同账号(登陆并领取优惠券)：对uid进行去重
    //6.2没有浏览商品：反面考虑，如果有浏览商品，当前mid不产生预警日志
    val couponAlertInfoDStream: DStream[CouponAlertInfo] = midToEventLogIterDStream.map { case (mid, logIter) => {

      //a.创建Set用于存放领券的UID
      val uidSet: util.HashSet[String] = new java.util.HashSet[String]()
      //创建用于存放领券涉及的商品ID
      val itemIds: util.HashSet[String] = new java.util.HashSet[String]()
      //创建List用于存放用户行为
      val events = new util.ArrayList[String]()

      //定义标志位用于标识是否有浏览行为
      var noClick: Boolean = true;

      //b.遍历logIter
      breakable(
        logIter.foreach(eventLog => {

          //提取时间类型
          val evid: String = eventLog.evid

          //将事件添加到集合
          events.add(evid)

          //判断当前数据是否有领券行为
          if ("coupon".equals(evid)) {
            itemIds.add(eventLog.itemid) //添加商品信息
            uidSet.add(eventLog.uid) //添加登陆领券用户
          } else if ("clickItem".equals(evid)) {
            noClick = false;
            break()
          }

        })
      )

      //根据条件选择生成预警日志
      if (uidSet.size() >= 3 && noClick) {
        //满足条件生产预警日志
        CouponAlertInfo(mid, uidSet, itemIds, events, System.currentTimeMillis())
      } else {
        //不满足条件生产预警日志
        null
      }
    }
    }

    //打印测试
    //couponAlertInfoDStream.print(100)

    //取出需预警的信息
    val filterAlertDStream: DStream[CouponAlertInfo] = couponAlertInfoDStream.filter(x => x != null)

    //预警日志测试打印
//    println("预警日志测试打印")
    filterAlertDStream.cache()
    filterAlertDStream.print()

    //7.将生成的预警日志写入ES
    filterAlertDStream.foreachRDD(rdd => {
      
      rdd.foreachPartition(iter => {

        //转换数据结构，预警日志=>(docID,预警日志)
        val docIdToData: Iterator[(String, CouponAlertInfo)] = iter.map(alertInfo => {
          val minutes: Long = alertInfo.ts / 1000 / 60
          (s"${alertInfo.mid}-$minutes", alertInfo)
        })

        //获取当前时间
        //获取当前时间
        val date: String = LocalDate.now().toString
        MyEsUtil.insertByBulk(GmallConstants.GMALL_ES_ALERT_INFO_PRE + "_" + date,
          "_doc",
          docIdToData.toList)
        
      })
      
    })


    //8.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
