package com.atguigu.app

import java.time.LocalDate
import java.util

import com.alibaba.fastjson.JSON
import com.atguigu.bean.{OrderDetail, OrderInfo, SaleDetail, UserInfo}
import com.atguigu.constant.GmallConstants
import com.atguigu.utils.{MyEsUtil, MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.json4s.native.Serialization

import collection.JavaConverters._
import scala.collection.mutable.ListBuffer

/**
 * @Author: Sdaer
 * @Date: 2020-08-19
 * @Desc:
 */
object SaleDetailApp {

  def main(args: Array[String]): Unit = {

    //1.创建StreamingContext
    val sparkConf = new SparkConf().setAppName("ScalaDetailApp").setMaster("local[*]")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    //2.消费Kafka order_info和order_detail主题数据
    val orderInfoKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_INFO, ssc)
    val orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstants.GMALL_TOPIC_ORDER_DETAIL, ssc)

    //3.将每一行数据转换为样例类元祖(order_id,order_info)(order_id,order_detail)
    val orderIdToInfoDStream: DStream[(String, OrderInfo)] = orderInfoKafkaDStream.map(record => {
      //a.转换为样例类对象
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
      //b.给日期及小时字段重新赋值
      val create_time: String = orderInfo.create_time //2020-08-18 04:24:04
      val timeArr: Array[String] = create_time.split(" ")
      orderInfo.create_date = timeArr(0)
      orderInfo.create_hour = timeArr(1).split(":")(0)
      //c.给联系人手机号脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
      orderInfo.consignee_tel = telTuple._1 + "*******"
      //d.返回数据
      (orderInfo.id, orderInfo)
    })

    val orderIdToDetailDStream: DStream[(String, OrderDetail)] = orderDetailKafkaDStream.map(record => {
      //JSON转换
      val detail: OrderDetail = JSON.parseObject(record.value(), classOf[OrderDetail])
      //返回数据
      (detail.order_id, detail)
    })

    //test4.做普通JOIN，数据延迟会丢失数据
//    val value: DStream[(String, (OrderInfo, OrderDetail))] = orderIdToInfoDStream.join(orderIdToDetailDStream)
//    val value1: DStream[(String, (OrderInfo, Option[OrderDetail]))] = orderIdToInfoDStream.leftOuterJoin(orderIdToDetailDStream)
//    val value2: DStream[(String, (Option[OrderInfo], OrderDetail))] = orderIdToInfoDStream.rightOuterJoin(orderIdToDetailDStream)
//    val value3: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //real4.考虑数据传输延迟进行Join
    //a.全外联获取数据，full Outer join
    val orderIdToInfoAndDetailDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] = orderIdToInfoDStream.fullOuterJoin(orderIdToDetailDStream)

    //4.1分区处理数据，防止频繁获取释放Redis连接
    val noUserSaleDetail: DStream[SaleDetail] = orderIdToInfoAndDetailDStream.mapPartitions(iter => {

      //获取Redis连接
      val jedisClient = RedisUtil.getJedisClient
      //创建集合用于存放JOIN上(包含当前批次，以及两个流中跟牵制批次Join上)的数据
      val details = new ListBuffer[SaleDetail]
      //导入样例类对象转化为JSON的隐式
      implicit val formats = org.json4s.DefaultFormats
      //val orderInfoJson: String = Serialization.write(orderInfo)

      //处理分区内的每条数据
      iter.foreach { case (orderId, (infoOpt, detailOpt)) =>

        //定义info&detail数据存入Redis中的key
        val infoRedisKey = s"order_info:$orderId"
        val detailRedisKey = s"order_detail:$orderId"

        //1)infoOpt有值
        if (infoOpt.isDefined) {
          //获取infoOpt中的数据
          val orderInfo: OrderInfo = infoOpt.get

          //1.1 detailOpt也有值
          if (detailOpt.isDefined) {
            //获取detailOpt中的数据
            val orderDetail: OrderDetail = detailOpt.get
            //结合放入集合
            details += new SaleDetail(orderInfo, orderDetail)
          }

          //1.2将orderInfo转换JSON字符串写入Redis
          //JSON.toJSONString(orderInfo)  编译报错
          val orderInfoJson = Serialization.write(orderInfo)
          jedisClient.set(infoRedisKey, orderInfoJson)
          jedisClient.expire(infoRedisKey, 100)

          //1.3查询OrderDetail流前置批次数据
          val orderDetailJsonSet: util.Set[String] = jedisClient.smembers(detailRedisKey)
          //import collection.JavaConverters._
          orderDetailJsonSet.asScala.foreach(orderDetailJson => {
            //将orderDetailJson转换为样例类对象
            val orderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
            //结合存放到集合
            details += new SaleDetail(orderInfo, orderDetail)
          })

        } else {
          //2.没有值

          //获取drtailOpt中的数据
          val orderDetail: OrderDetail = detailOpt.get

          //查询OrderInfo流牵制批次数据
          val orderInfoJson: String = jedisClient.get(infoRedisKey)
          if (orderInfoJson != null) {
            //2.1查询有数据
            //将orderDetailJson转换为样例类对象
            //JSON转换为样例类是根据字段转化的
            val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
            //结合写出
            details += new SaleDetail(orderInfo, orderDetail)
          } else {
            //2.2查询没有结果，将当前的orderDetail写入Redis
            val orderDetailJson: String = Serialization.write(orderDetail)
            jedisClient.sadd(detailRedisKey, orderDetailJson)
            jedisClient.expire(detailRedisKey, 100)
          }
        }
      }

      //释放连接
      jedisClient.close()
      //最终的返回值
      details.toIterator
    })

    //根据userID查询Redis数据，将用户信息补充完整
    val saleDetailDStream: DStream[SaleDetail] = noUserSaleDetail.mapPartitions(iter => {

      //a.获取Redis连接
      val jedisClient = RedisUtil.getJedisClient
      //b.遍历iter，对每一条数据进行查询Redis操作，补充用户信息
      val details = iter.map(noUserSaleDetail => {
        //查询Redis
        val userInfoJson = jedisClient.get(s"userInfo:${noUserSaleDetail.user_id}")
        //将UserInfoJson转化为样例类
        val userInfo: UserInfo = JSON.parseObject(userInfoJson, classOf[UserInfo])
        //补充信息
        noUserSaleDetail.mergeUserInfo(userInfo)
        //返回数据
        noUserSaleDetail
        //
      })

      //释放连接
      jedisClient.close()

      //返回数据
      details

    })

    //将三张表JOIN的结果写入ES
    saleDetailDStream.foreachRDD(rdd => {

      rdd.foreachPartition(iter => {

        //根据当天的时间建立索引名称
        val today = LocalDate.now().toString
        val indexName: String = s"${GmallConstants.GMALL_ES_SALE_DETAIL_PRE}-$today"

        //将orderDetailId作为ES中索引的docID
        val detailIdToSaleDetailIter: Iterator[(String, SaleDetail)] = iter.map(saleDetail => (saleDetail.order_detail_id,saleDetail))

        //调用ES工具栏写入数据
        MyEsUtil.insertByBulk(indexName,"_doc",detailIdToSaleDetailIter.toList)

      })

    })

    //5.打印测试
    //value.print(100)
    //noUserSaleDetail.print(100) //此处print会打印时间戳
//    saleDetailDStream.print(100)

    //6.启动任务
    ssc.start()
    ssc.awaitTermination()

  }

}
