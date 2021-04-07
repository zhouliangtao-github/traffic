package main

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}
import utils.RedisUtil

import scala.collection.mutable.ArrayBuffer

/**
 * 数据建模
 */
object Train {
  def main(args: Array[String]): Unit = {
    //评估结果保存
    val writer = new PrintWriter(new File("modeltraining.txt"))

    //配置spark
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("Traffictraining")
    val sc = new SparkContext(sparkConf)

    //建立redis链接，相关
    val dbIndex = 1
    val jedis = RedisUtil.pool.getResource
    jedis.select(dbIndex)

    //设置想要对哪个检测点 数据建模,一个卡口一个模型，训练后，去拟合后面的数据 lbfgs算法与 sgd随机梯度下降 Gd梯度下降（下山）
    val monitorIDs = List("0005", "0015")
    //对上面2卡口建模，但他们可能需要其他检查点的数据信息
    val monitorRelations = Map[String, Array[String]](
      "0005" -> Array("0003", "0004", "0006", "0007"),
      "0015" -> Array("0013", "0014", "0016", "0017")
    )

    //遍历上诉所有监测点，进行建模
    monitorIDs.map(monitorID => {
      //"0003","0004","0005","0006","0007"
      val monitorRelationList = monitorRelations.get(monitorID).get
      //处理时间
      val currentDate = Calendar.getInstance.getTime
      //设置时间格式
      val hourMinuteSDF = new SimpleDateFormat("HHmm")
      val dateSDF = new SimpleDateFormat("yyyyMMdd")
      val dateOfString = dateSDF.format(currentDate) //20210401

      //根据相关检测点取得所有当天信息
      val relationInfo = monitorRelationList.map(monitorID =>
        (monitorID, jedis.hgetAll(dateOfString + "_" + monitorID)))

      //使用n个小时内的数据进行建模
      val hours = 1

      val dataTrain = ArrayBuffer[LabeledPoint]() //有监督学习
      val dataX = ArrayBuffer[Double]() //存特征因子
      val dataY = ArrayBuffer[Double]() //存特征因子对应结果集

      //将时间拉回1小时之前    //60,59,58,...3分钟
      for (i <- Range(60 * hours, 2, -1)) {
        dataX.clear
        dataY.clear

        for (index <- 0 to 2) {
          val oneMoment = currentDate.getTime - 60 * i * 1000 + 60 * index * 1000
          val oneHM = hourMinuteSDF.format(new Date(oneMoment))

          for ((k, v) <- relationInfo) {
            if (k == monitorID && index == 2) {
              //第四分钟数据
              val nextMoment = oneMoment + 60 * 1000
              val nextHM = hourMinuteSDF.format(new Date(oneMoment))

              if (v.containsKey(nextHM)) {
                val speedAndCarCount = v.get(nextHM).split("_")
                val valueY = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataY += valueY
              }

              if (v.containsKey((oneHM))) {
                val speedAndCarCount = v.get(oneHM).split("_")
                val valueX = speedAndCarCount(0).toFloat / speedAndCarCount(1).toFloat
                dataX += valueX
              } else {
                dataX += -1.0F
              }
            }
          }
          //训练模型
          if (dataY.toArray.length == 1) {
            val label = dataY.toArray.head
            val record = LabeledPoint(if (label.toInt / 10 < 10) label.toInt / 10 else 10, Vectors.dense(dataX.toArray))
          }
          dataTrain.foreach(println(_))
          val rddData = sc.parallelize(dataTrain)

          //切分数据，训练测试集
          val randomSplits = rddData.randomSplit(Array(0.6, 0.4), 11L)
          val trainingData = randomSplits(0)
          val testData = randomSplits(1)

          if (!rddData.isEmpty()) {
            val model: LogisticRegressionModel = new LogisticRegressionWithLBFGS().setNumClasses(11).run(trainingData)
            val predictionAndLabels = testData.map {
              case LabeledPoint(label, features) => val prediction = model.predict(features); (prediction, label)
            }
            //得到当前检测点model的评估值
            val metrics = new MulticlassMetrics(predictionAndLabels)
            val accuracy = metrics.accuracy
            println("评估值：" + accuracy)
            writer.write(accuracy.toString + "\r\n")
            //test 0.0
            if (accuracy > 0.9) {
              val hdfsPath = "hdfs://linux01:8020/traffic/model" + monitorID + "_" + new SimpleDateFormat("yyyyMMddHHmmss").format(currentDate.getTime)
              model.save(sc, hdfsPath)
              jedis.hset("model", monitorID, hdfsPath)
            }
          }
        }
      }

      RedisUtil.pool.returnResource(jedis)
      writer.close
    })
  }
}
