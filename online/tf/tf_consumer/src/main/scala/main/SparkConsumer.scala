package main

import java.text.SimpleDateFormat
import java.util.Calendar

import com.alibaba.fastjson.{JSON, TypeReference}
import io.netty.handler.codec.string.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}
import utils.{PropertyUtil, RedisUtil}

object SparkConsumer {
  def main(args: Array[String]): Unit = {
    //初始化spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("TrafficStreaming")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("./ssc/checkpoint")

    //配置kafka参数
    val kafkaParams = Map("metadata.broker.list" -> PropertyUtil.getProperty("metadata.broker.list"))

    //想要消费的数据在哪个主题？
    val topics = Set(PropertyUtil.getProperty("kafka.topics"))

    //读取kakfa中value的数据
    val kafkaLineDStream = KafkaUtils.createDirectStream[
      String,
      String,
      StringDecoder,
      StringDecoder](ssc, kafkaParams, topics)
      .map(_._2)

    //解析我们读到的数据
    val event = kafkaLineDStream.map(line => {
      //Json解析
      val lineJavaMap = JSON.parseObject(line, new TypeReference[java.util.Map[String, String]]() {})

      //将JavaMap转为ScalaMap
      import scala.collection.JavaConverters._
      val lineScalaMap: collection.mutable.Map[String, String] = mapAsScalaMapConverter(lineJavaMap).asScala
      println(lineScalaMap)
      lineScalaMap
    })
    //将数据简单聚合：tuple类型
    //目标数据,("monitor_id":"0001","speed":"50")
    //目标，（0001，（500，10）  （卡口，（1分钟内总车速，总车数）
    val sumOfSpeedAndCount = event.map(e => (e.get("monitor_id").get, e.get("speed").get)) //("0001","00")
      .mapValues(v => (v.toInt, 1)) //("0001",(50,1))
      .reduceByKeyAndWindow((t1: (Int, Int), t2: (Int, Int)) =>
        (t1._1 + t2._1, t1._2 + t2._2), Seconds(20), Seconds(10)) //实际都得改为60，攒60skafka的数据时间窗口(宽度) ("0001",(500,10))

    //将上面数据存于redis中
    val dbIndex = 1
    sumOfSpeedAndCount.foreachRDD(rdd => {
      rdd.foreachPartition(partitionRecord => {
        partitionRecord
          .filter((tuple: (String, (Int, Int))) => tuple._2._2 > 0)
          .foreach(pair => {
            val jedis = RedisUtil.pool.getResource
            val monitorId = pair._1 //"0001"
            val sumOfSpeed = pair._2._1 //500
            val sumOfCarCount = pair._2._2 //10

            //将数据实时保存redis中
            val currentTime = Calendar.getInstance().getTime
            //HHmm
            val hmSDF = new SimpleDateFormat("HHmm")
            //yyyyMMdd
            val dateSDF = new SimpleDateFormat("yyyyMMdd")

            //对时间格式化
            val hourMinuteTime = hmSDF.format(currentTime) //"2036"
            val date = dateSDF.format(currentTime)

            jedis.select(dbIndex)
            //key  ---> 20210401_0001
            //fields --->   2036
            //value ---->    500_10
            jedis.hset(date + "_" + monitorId, hourMinuteTime, sumOfSpeed + "_" + sumOfCarCount)
            jedis.close()
          })
      })
    })

    ssc.start
    ssc.awaitTermination()
  }
}
