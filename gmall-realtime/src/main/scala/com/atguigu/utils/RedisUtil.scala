package com.atguigu.utils

import java.util.Properties

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
 * @Author: Sdaer
 * @Date: 2020-08-16
 * @Desc:
 *      Redis连接获取工具类
 *      1.先开辟一个连接池(JedisPool)
 *         1.1 读取配置文件，获得地址和端口
 *         1.2 配置连接要素
 *      2.再从连接池中获取一个连接
 */
object RedisUtil {

  //定义RedisPool
  var jedisPool: JedisPool = _

  //从池子获取连接
  def getJedisClient:Jedis = {

    if (jedisPool == null){

      println("开辟一个连接池")
      //加载配置文件
      val config: Properties = PropertiesUtil.load("config.properties")
      //从配置文件获取redis的主机和端口号
      val host: String = config.getProperty("redis.host")
      val port: String = config.getProperty("redis.port")

      //创建连接池
      //创建连接池配置对象
      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100)  //最大连接数
      jedisPoolConfig.setMaxIdle(20)    //最大空闲:表示即使没有连接时依然可以保持20空闲的连接
      jedisPoolConfig.setMinIdle(20)    //最小空闲:表示即使没有连接时依然可以保持20空闲的连接
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWaitMillis(500)   //忙碌时等待时长 毫秒
      jedisPoolConfig.setTestOnBorrow(true)   //每次获得连接时进行测试，是否有连接可用

      //根据配置创建连接池
      jedisPool = new JedisPool(jedisPoolConfig,host,port.toInt)
    }
    //println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")  //打印活跃连接数
    //println("获取一个redis连接")
    jedisPool.getResource //从连接池获取一个连接
  }

}
