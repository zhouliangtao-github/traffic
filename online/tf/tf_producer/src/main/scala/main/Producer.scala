package main

import java.text.DecimalFormat
import java.util.Calendar

import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import utils.PropertyUtil

import scala.util.Random

/**
 * 需求：我想知道9.30这个路段会不会堵车
 */
object Producer {
  def main(args: Array[String]): Unit = {
    //读取Kafka配置信息
    val props = PropertyUtil.properties
    //创建kafka生产者对象
    val producer = new KafkaProducer[String, String](props)

    //模拟生产实时数据（既有堵车又有顺畅），切换一次车辆速度/5分钟
    //当前时间：单位：秒
    var startTime = Calendar.getInstance().getTimeInMillis / 1000

    val trafficCycle = 300
    //不停的开始模拟数据
    while (true) {
      //模拟生产检查点(卡口id)将int转化为字符串
      val randomMonitorId = new DecimalFormat("0000").format(Random.nextInt(20) + 1)

      //定义一个车速
      var randomSpeed = ""

      //数据模拟（堵车）切换周期，5分钟
      val currentTime = Calendar.getInstance().getTimeInMillis / 1000

      if (currentTime - startTime > 300) { //第2个5分钟
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(16)) //[0,16)堵车
        if (currentTime - startTime > trafficCycle * 2) { //第3个5分钟，重置时间
          startTime = currentTime
        }
      } else {
        randomSpeed = new DecimalFormat("000").format(Random.nextInt(31) + 30) //[30,60]第1,3个5分钟
      }

      val jsonMap = new java.util.HashMap[String, String]()
      jsonMap.put("monitor_id", randomMonitorId)
      jsonMap.put("speed", randomSpeed)

      //序列化Json
      val event = JSON.toJSON(jsonMap)
      println(event)

      //把消息发送到kafka集群
      producer.send(new ProducerRecord[String, String](props.getProperty("kafka.topics"), event.toString))
      Thread.sleep(200)
    }
  }
}

