package com.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object JedisConnectionPool {

  private val config = new JedisPoolConfig()

  // 最大连接数
  config.setMaxTotal(20)
  // 最大空闲连接数
  config.setMaxIdle(10)
  // 加载config
  private val pool = new JedisPool(config,"192.168.28.131",6379,10000,"123")
  //获取连接
  def getConnection():Jedis={
    pool.getResource
  }
}
