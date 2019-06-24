package com.ml

import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.{ProductToUser, Recommendation, RecommendationUser, UserToProduct}
import com.ml.util.MongoUtil
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.sql.{DataFrame, Row}

/**
  * Project: ECSystem
  * ClassName: com.ml.OfflineRecommender
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 9:50
  */
object OfflineRecommender extends SparkSess {
  def main(args: Array[String]): Unit = {


    import spark.implicits._
    val ratingRDD = rating.rdd.map(line => (line.getAs[Int]("userId"), line.getAs[Int]("productId"), line.getAs[Double]("score")))
    //训练隐语义模型
    val trainData = ratingRDD.map(line => (line._1, line._2, line._3)).toDF("userId", "productId", "score")

    //als建立推荐模型，显性反馈  代表偏好程度
    val als = new ALS().setMaxIter(5).setRegParam(0.02).setImplicitPrefs(false).setNonnegative(true).setAlpha(0.02).setRank(50)
      .setUserCol("userId")
      .setItemCol("productId")
      .setRatingCol("score")


    //训练模型(显性)
    val model=als.fit(trainData)
    //冷启动处理：Spark允许用户将coldStartStrategy参数设置为“drop”,以便在包含NaN值的预测的DataFrame中删除任何行
    model.setColdStartStrategy("drop")

    //为每个用户提供10大电影排名
    val movie:DataFrame= model.recommendForAllUsers(50)

    val userRecs = movie.rdd.map((line:Row)=> {
      val recommList: Seq[(Int ,Float)] = line.getAs[Seq[Row]](1).map(x =>{
        (x.getInt(0),x.getFloat(1))
      })
      UserToProduct(line.getInt(0) ,recommList.toList.map(x =>Recommendation(x._1 ,x._2)))
    }).toDF()
    MongoUtil.insertDFInMongoDB(userRecs,GlobalConstant.USER_TO_PRODUCT)

    //为每个电影推荐10个候选人
    val movieUser = model.recommendForAllItems(50)

    val movieRecs = movieUser.rdd.map((line:Row)=> {
      val recommList: Seq[(Int ,Float)] = line.getAs[Seq[Row]](1).map(x =>{
        (x.getInt(0),x.getFloat(1))
      })

      ProductToUser(line.getInt(0) ,recommList.toList.map(x =>RecommendationUser(x._1 ,x._2)))
    }).toDF()

    MongoUtil.insertDFInMongoDB(movieRecs,GlobalConstant.PRODUCT_TO_USER)

  }
}



