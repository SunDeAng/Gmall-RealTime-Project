package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constant.GmallConstants
import com.atguigu.handler.DauHandler
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.phoenix.spark._

/**
 * @Author: Sdaer
 * @Date: 2020-08-16
 * @Desc:
 */
object DauApp {

  def main(args: Array[String]): Unit = {

    //1.创建SparkContext
    val sparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

    //2.创建StreamingContext
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //3.读取kafka start主题的数据创建流
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.KAFKA_TOPIC_STARTUP, ssc)

    //4.将读取的数据转化为样例类对象(logDate和logHour)
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startUpLogStream: DStream[StartUpLog] = kafkaDStream.map(record => {

      //a.取出value
      val value = record.value()

      //b.转换为样例类对象
      val startUplog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])

      //c.取出时间戳字段解析给logDate和logHour赋值
      val ts: Long = startUplog.ts
      val dataHour: String = sdf.format(new Date(ts))
      val dateHourArr = dataHour.split(" ")
      startUplog.logDate = dateHourArr(0)
      startUplog.logHour = dateHourArr(1)

      //d.返回值
      startUplog

    })

    //多次使用同一个计算后的值，需缓存提高性能
//    startUpLogStream.cache()
//    startUpLogStream.count().print()

    //5.根据Redis中保持的数据进行跨批次处理
    val filterByRedisDStream: DStream[StartUpLog] = DauHandler.filterByRedis(startUpLogStream, ssc.sparkContext)

//    filterByRedisDStream.cache()
//    filterByRedisDStream.count().print()

    //6.对第一次去重后的数据做同批次去重
    val filterByGroupDStream: DStream[StartUpLog] = DauHandler.filterByGroup(filterByRedisDStream)

    filterByGroupDStream.cache()
//    filterByGroupDStream.count().print()

    //7.将两次去重后的数据(mid)写入Redis,注意第一次启动需先将数据加载到redis，即先于5执行
    DauHandler.saveMidToRedis(filterByGroupDStream)

    //8.将数据保存到HBase(Phoenix)
    filterByGroupDStream.foreachRDD(rdd => {
        rdd.saveToPhoenix("GMALL200317_DAU",
          classOf[StartUpLog].getDeclaredFields.map(_.getName.toUpperCase),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181")
        )
    })

    //9.启动任务
    ssc.start()
    ssc.awaitTermination()


  }

}
