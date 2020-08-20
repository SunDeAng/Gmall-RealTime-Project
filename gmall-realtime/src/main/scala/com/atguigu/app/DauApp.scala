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
 *       日活实时需求
 *       1、数据流程
 *          1）创建SparkConf配置
 *          2）根据SparkConf创建Sparkstreaming
 *          3）从Kafka工具类获取主题消费流
 *          4）从流中获取每一天数据转换成样例类，并添加两个时间字段
 *          5)数据先会根据Redis进行跨批次去重
 *          6）然后将跨批次去重的结果进行同批次去重
 *          7）将去重后的数据中mid先保存到redis以备下次去重使用
 *          8）将去重后的所有数据保存到HBase
 *       2、注意点
 *          1）为什么要先跨批次去重，再同批次去重
 *              数据清洗的原则是首先执行去重力度大的操作
 *              跨批次去重的清洗力度比同批次去重的力度大很多
 *              很少有人同批次(5S)登陆2词
 *          2）mid为什么保存到redis
 *              首先redis是保存数据在内存的，读写速度很快
 *              其次redis中的set特性可以用作去重操作
 *          3）为什么数据不全部保存到redis，而是保存到HBase
 *              首先数据保存到Redis，虽然读写快，但对内存要求高。
 *              其次我们在数据中添加了分时字段，把数据保存在HBase中可以按照维度进行分析
 *        3、优化点
 *          1）数据转换为样例类
 *          2）避免重复获取释放连接
 *              我们可以根据第5步的实现可以发现有三种实现方法
 *              第一种，每条数据都会获取释放Redis连接
 *              第二种，每个分区都会获取释放Redis连接
 *              第三种，可以从一个Redis连接中获取当天所有数据，然后用广播变量，这样一批数据就只要一个连接
 *          3）RDD缓存
 *              如果一个RDD需多次使用，需缓存下来，提高使用速度
 *
 *
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
