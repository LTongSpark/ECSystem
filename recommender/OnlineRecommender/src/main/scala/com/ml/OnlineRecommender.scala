package com.ml

import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.MongoConfig
import com.ml.repository.ViolationsRepository
import com.ml.util.{JPools, MongoUtil}
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010._
import redis.clients.jedis.Jedis

/**
  * Project: ECSystem
  * ClassName: com.ml.OnlineRecommender
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 9:56
  */

object OnlineRecommender extends SparkSess {
  def main(args: Array[String]): Unit = {
    //问了方便查询  转换成map
    // 加载电影相似度矩阵数据，把它广播出去
    val simProductsMatrix: collection.Map[Int, scala.collection.immutable.Map[Int, Double]] =
    MongoUtil.loadMovieRecDFInMongoDB(spark, GlobalConstant.PRODUCT_RECS)

    // 定义广播变量
    val simProcutsMatrixBC: Broadcast[collection.Map[Int, Map[Int, Double]]] = sc.broadcast(simProductsMatrix)

    //从redis中获取偏移量
    val offsetMap = Map[TopicPartition, Long]()

    //链接到kafka的数据源
    val kafkaStream: InputDStream[ConsumerRecord[String, String]] = if (offsetMap.size == 0) {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Subscribe[String, String](Array(ViolationsRepository.kafkaConfig("kafka.topic")), ViolationsRepository.kafkaParam))
    } else {
      KafkaUtils.createDirectStream[String, String](
        ssc,
        LocationStrategies.PreferConsistent,
        ConsumerStrategies.Assign[String, String](offsetMap.keys, ViolationsRepository.kafkaParam, offsetMap))
    }

    // 把原始数据UID|MID|SCORE|TIMESTAMP 转换成评分流
    val ratingStream = kafkaStream.map {
      msg =>
        val attr = msg.value().split("\\|")
        (attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }
    ratingStream.foreachRDD(rdds => {
      //判断rdd非空
      if (!rdds.isEmpty()) {
        val offsetRange: Array[OffsetRange] = rdds.asInstanceOf[HasOffsetRanges].offsetRanges
        rdds.foreach(rdd => {
          println("rating data coming! >>>>>>>>>>>>>>>>")
          // 1. 从redis里获取当前用户最近的K次评分，保存成Array[(mid, score)]  uid, mid, score, timestamp
          val userRecentlyRatings = getUserRecentlyRatings(GlobalConstant.MAX_USER_RATING_NUM, rdd._1, JPools.getJedis)

          // 2. 从相似度矩阵中取出当前电影最相似的N个电影，作为备选列表，Array[mid]
          val candidateMovies = getTopSimProducts(GlobalConstant.MAX_SIM_PRODUCTS_NUM, rdd._2, rdd._1, simProcutsMatrixBC.value)

          // 3. 对每个备选电影，计算推荐优先级，得到当前用户的实时推荐列表，Array[(mid, score)]
          val streamRecs = computeProductScore(candidateMovies, userRecentlyRatings, simProcutsMatrixBC.value)
          //将推荐数据保存在mongodb
          MongoUtil.saveDataToMongoDB(rdd._1, streamRecs)

        })
        //将偏移量存到redis
        val jedis = JPools.getJedis
        jedis.select(7)
        for (offset <- offsetRange) {
          //  yc-info-0
          jedis.hset(ViolationsRepository.kafkaConfig("group.id"), offset.topic + "-" + offset.partition, offset.untilOffset.toString)
        }
      }
      //启动程序
      ssc.start()
      ssc.awaitTermination()
    })

    /**
      * 从redis里获取最近num次评分
      */
    import scala.collection.JavaConversions._
    def getUserRecentlyRatings(num: Int, userId: Int, jedis: Jedis): Array[(Int, Double)] = {
      // 从redis中用户的评分队列里获取评分数据，list键名为uid:USERID，值格式是 PRODUCTID:SCORE
      jedis.select(8)
      jedis.lrange("userId:" + userId.toString, 0, num)
        .map { item =>
          val attr = item.split("\\:")
          (attr(0).trim.toInt, attr(1).trim.toDouble)
        }
        .toArray
    }

    // 获取当前商品的相似列表，并过滤掉用户已经评分过的，作为备选列表
    def getTopSimProducts(num: Int,
                          productId: Int,
                          userId: Int,
                          simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
                         (implicit mongoConfig: MongoConfig): Array[Int] = {
      // 从广播变量相似度矩阵中拿到当前商品的相似度列表
      val allSimProducts = simProducts(productId).toArray

      // 获得用户已经评分过的商品，过滤掉，排序输出
      val ratingExist = MongoClient(MongoClientURI(mongoConfig.uri))(mongoConfig.db)(GlobalConstant.MONGODB_RATING_COLLECTION)
        .find(MongoDBObject("userId" -> userId))
        .toArray
        .map { item => // 只需要productId
          item.get("productId").toString.toInt
        }
      // 从所有的相似商品中进行过滤
      allSimProducts.filter(x => !ratingExist.contains(x._1))
        .sortWith(_._2 > _._2)
        .take(num)
        .map(x => x._1)
    }

    // 计算每个备选商品的推荐得分
    def computeProductScore(candidateProducts: Array[Int],
                            userRecentlyRatings: Array[(Int, Double)],
                            simProducts: scala.collection.Map[Int, scala.collection.immutable.Map[Int, Double]])
    : Array[(Int, Double)] = {
      // 定义一个长度可变数组ArrayBuffer，用于保存每一个备选商品的基础得分，(productId, score)
      val scores = scala.collection.mutable.ArrayBuffer[(Int, Double)]()
      // 定义两个map，用于保存每个商品的高分和低分的计数器，productId -> count
      val increMap = scala.collection.mutable.HashMap[Int, Int]()
      val decreMap = scala.collection.mutable.HashMap[Int, Int]()

      // 遍历每个备选商品，计算和已评分商品的相似度
      for (candidateProduct <- candidateProducts; userRecentlyRating <- userRecentlyRatings) {
        // 从相似度矩阵中获取当前备选商品和当前已评分商品间的相似度
        val simScore: Double = simProducts.getOrElse(candidateProduct, 0.0).asInstanceOf[Map[Int, Double]]
          .getOrElse(userRecentlyRating._1, 0.0)
        if (simScore > 0.4) {
          // 按照公式进行加权计算，得到基础评分
          scores += ((candidateProduct, simScore * userRecentlyRating._2))
          if (userRecentlyRating._2 > 3) {
            increMap(candidateProduct) = increMap.getOrDefault(candidateProduct, 0) + 1
          } else {
            decreMap(candidateProduct) = decreMap.getOrDefault(candidateProduct, 0) + 1
          }
        }
      }

      // 根据公式计算所有的推荐优先级，首先以productId做groupby
      scores.groupBy(_._1).map {
        case (productId, scoreList) =>
          (productId, scoreList.map(_._2).sum / scoreList.length +
            math.log10(increMap.getOrDefault(productId, 1))
            - math.log10(decreMap.getOrDefault(productId, 1)))
      }
        // 返回推荐列表，按照得分排序
        .toArray
        .sortBy(-_._2)
    }

  }
}
