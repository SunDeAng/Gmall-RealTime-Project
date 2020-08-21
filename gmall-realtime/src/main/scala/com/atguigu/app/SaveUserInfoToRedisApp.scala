package com.atguigu.app

import com.alibaba.fastjson.JSON
import com.atguigu.bean.UserInfo
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.{MyKafkaUtil, RedisUtil}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * @Author: Sdaer
 * @Date: 2020-08-21
 * @Desc:
 */
object SaveUserInfoToRedisApp {

  def main(args: Array[String]): Unit = {

    //1.创建SSC
    val conf = new SparkConf().setAppName("SaleDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(conf,Seconds(5))

    //2.读取Kafka中USER_INFO主题的数据
    val kafkaDStream = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_USER_INFO, ssc)

    //3.写入Redis
    kafkaDStream.foreachRDD(rdd => {

      //对分区进行操作，减少连接的创建和释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient = RedisUtil.getJedisClient

        //b.变量iter，写入Redis
        iter.foreach(record => {
          //获取数据
          val userInfoJson = record.value()
          //转换为样例类对象
          val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
          //定义RedisKey
          val userRedisKey = s"userInfo:${userInfo.id}"
          //将数据吸热Redis
          jedisClient.set(userRedisKey,userInfoJson)
        })

        //c.释放连接
        jedisClient.close()

      })

    })

    //4.启动任务
    ssc.start()
    ssc.awaitTermination()


  }

}
