package com.ml

import java.text.SimpleDateFormat
import java.util.Date

import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.{MongoConfig, Rating}
import com.ml.util.{DateUtil, MongoUtil}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
/**
  * Project: ECSystem
  * ClassName: com.ml.StatisticsRecommender
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 9:54
  */
object StatisticsRecommender extends SparkSess{

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    // 创建一张叫ratings的临时表
    rating.createOrReplaceTempView("ratings")

    // 1. 历史热门商品，按照评分个数统计，productId，count
    val rateMoreProductsDF = rating.groupBy(rating.col("productId"))
      .agg(count(rating.col("productId")).as("count"))
      .select("productId" ,"count")
    MongoUtil.insertDFInMongoDB( rateMoreProductsDF, GlobalConstant.RATE_MORE_PRODUCTS )


    // 2. 近期热门商品，把时间戳转换成yyyyMM格式进行评分个数统计，最终得到productId, count, yearmonth
    val ratingOfYearMonth = rating.select(rating.col("productId") ,rating.col("score"),
      rating.col("timestamp")).rdd.map(data =>{
      (data.getAs[Int]("productId") , data.getAs[Double]("score"),
        DateUtil.parse(data.getAs[Int]("timestamp")))
    }).toDF("productId","score","yearmonth")

    val rateMoreRecentlyProductsDF = ratingOfYearMonth.groupBy("yearmonth","productId")
      .agg(count(ratingOfYearMonth.col("productId")).as("count"))
      .orderBy(ratingOfYearMonth.col("yearmonth").desc,$"count".desc)
      .select("productId" ,"count" ,"yearmonth")
    // 把df保存到mongodb
    MongoUtil.insertDFInMongoDB( rateMoreRecentlyProductsDF, GlobalConstant.RATE_MORE_RECENTLY_PRODUCTS )

    // 3. 优质商品统计，商品的平均评分，productId，avg
    val averageProductsDF = rating.groupBy(rating.col("productId"))
      .agg(avg(rating.col("score")).as("avg"))
      .select("productId" ,"avg")
    MongoUtil.insertDFInMongoDB( averageProductsDF, GlobalConstant.AVERAGE_PRODUCTS )

    spark.stop()
  }

}

