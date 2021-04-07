package utils

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {
  val host = "192.168.216.20"
  val port =6379
  val timeout = 30000

  val config= new JedisPoolConfig
  config.setMaxIdle(50)
  config.setMaxTotal(200)
  config.setMinIdle(0)
  config.setMaxWaitMillis(10000)
  config.setTestOnBorrow(true)
  config.setTestOnReturn(true)
  config.setTestOnCreate(true)

  config.setTestWhileIdle(true)
  config.setTimeBetweenEvictionRunsMillis(30000)
  config.setNumTestsPerEvictionRun(10)
  config.setMinEvictableIdleTimeMillis(60000)

  //连接池
  lazy val pool =new JedisPool(config,host,port,timeout)

  //创建程序崩溃时，回收资源的线程
  lazy val hook =new Thread{
    override def run(): Unit = {
      pool.destroy()
    }
  }
  sys.addShutdownHook(hook)
}
