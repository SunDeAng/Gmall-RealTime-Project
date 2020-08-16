package com.atguigu.handler

import java.{lang, util}
import java.time.LocalDate

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

/**
 * @Author: Sdaer
 * @Date: 2020-08-16
 * @Desc:
 */
object DauHandler {

  //对第一次去重后的数据做同批次去重
  def filterByGroup(filterByRedisDStream: DStream[StartUpLog]) : DStream[StartUpLog] = {
    //a.转换结构
    val midDateToStartLogDStream: DStream[(String, StartUpLog)] = filterByRedisDStream.map(startLog => {
      (s"${startLog.mid}-${startLog.logDate}", startLog)
    })


    //b.按照key进行分组
    val midDateToStartLogDStreamIter: DStream[(String, Iterable[StartUpLog])] = midDateToStartLogDStream.groupByKey()

    //c.组内去时间戳最小的一条数据
    val midDateToStartLogDStreamList: DStream[(String, List[StartUpLog])] = midDateToStartLogDStreamIter.mapValues(iter => {
      iter.toList.sortWith(_.ts < _.ts).take(1)
    })

    //d.压平
    midDateToStartLogDStreamList.flatMap{case (_,list) => list}

  }




  //根据Redis中保持的数据进行跨批次处理
  def filterByRedis(startUpLogStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //方案一：单条数据过滤
    //局限性：每条都要获取释放连接，造成资源浪费
    val value1 = startUpLogStream.filter(startLog => {
      //a.获取redis连接
      val jedisClient = RedisUtil.getJedisClient
      //b.查询Redis中是否存在该MID
      val exist: lang.Boolean = jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      //c.归还连接
      jedisClient.close()
      //d.返回值
      !exist //数据在redis中存在，说明为true，则需在此处返回false表示此条数据丢弃
    })

    //方案二：使用分区操作代替单条数据操作，减少连接数
    //局限：每批数据分发，使用广播变量好点
    val value2 = startUpLogStream.mapPartitions(iter => {
      //a.获取连接
      val jedisClient = RedisUtil.getJedisClient
      //b.过滤数据
      val filterIter: Iterator[StartUpLog] = iter.filter(startLog => {
        !jedisClient.sismember(s"dau:${startLog.logDate}", startLog.mid)
      })
      //c.归还连接
      jedisClient.close()
      //d.返回值
      filterIter
    })

    //方案三：每个批次获取一次Redis中的Set集合数据，广播至Executor
    val value3 = startUpLogStream.transform(rdd => {

      //a.获取Redis中的set集合并广播，每个批次在Driver端执行一次
      val jedisClient = RedisUtil.getJedisClient

      val today: String = LocalDate.now().toString
      val midSet = jedisClient.smembers(s"dau:$today")
      val midSetBC: Broadcast[util.Set[String]] = sc.broadcast(midSet)

      jedisClient.close()

      //b.在Executor端使用广播变量进行去重
      rdd.filter(startLog => {
        !midSetBC.value.contains(startLog.mid)
      })
    })
    //value1  //测试方法1的返回
    //value2  //测试方法2的返回
    value3    //测试方法3的返回


  }


  //对两次去重后的数据(mid)写入Reids
  def saveMidToRedis(startUpLogStream: DStream[StartUpLog]) = {

    startUpLogStream.foreachRDD(rdd => {

      //使用foreachPartition代替foreach，减少连接的获取和释放
      rdd.foreachPartition(iter => {

        //a.获取连接
        val jedisClient: Jedis = RedisUtil.getJedisClient

        //b.操作数据
        iter.foreach(startUpLog => {
          //给redis存储的key打标记，给是为dau:时间
          val redisKey: String = s"dau:${startUpLog.logDate}"
          //注意value选型的三大要素
          jedisClient.sadd(redisKey,startUpLog.mid)
        })

        //c.归还连接
        jedisClient.close()

      })

    })

  }

}
