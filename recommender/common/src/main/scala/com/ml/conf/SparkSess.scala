package com.ml.conf

import com.ml.common.GlobalConstant
import com.ml.domain.MongoConfig
import com.ml.repository.ViolationsRepository
import com.ml.util.MongoUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Project: ECSystem
  * ClassName: com.ml.conf.SparkSess
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 10:45
  */
trait SparkSess {
  val spark:SparkSession = SparkSession.builder()
    .appName(this.getClass.getSimpleName)
    .master("local[6]")
    .getOrCreate()
  implicit val mongoConfig = MongoConfig(ViolationsRepository.mongoConfig("mongo.uri"),ViolationsRepository.mongoConfig("mongo.db"))
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc,Seconds(20))

  //加载movie数据
  val product = MongoUtil.loadProductDFInMongoDB(spark,GlobalConstant.MONGODB_PRODUCT_COLLECTION)
  // 加载数据
  val rating = MongoUtil.loadRatingDFInMongoDB(spark,GlobalConstant.MONGODB_RATING_COLLECTION)



}
