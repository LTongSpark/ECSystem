package com.ml.util

import com.ml.common.GlobalConstant
import com.ml.domain.{MongoConfig, Product, ProductRecs, Rating}
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Project: ECSystem
  * ClassName: com.ml.util.MongoUtil
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 11:25
  */
object MongoUtil {
  /**
    * 保存数据到mongodb中
    * @param productDF
    * @param ratingDF
    * @param mongoConfig
    */
  def storeDataInMongoDB( productDF: DataFrame, ratingDF: DataFrame )(implicit mongoConfig: MongoConfig): Unit ={
    // 新建一个mongodb的连接，客户端
    val mongoClient = MongoClient( MongoClientURI(mongoConfig.uri) )
    // 定义要操作的mongodb表，可以理解为 db.Product
    val productCollection = mongoClient( mongoConfig.db )( GlobalConstant.MONGODB_PRODUCT_COLLECTION )
    val ratingCollection = mongoClient( mongoConfig.db )( GlobalConstant.MONGODB_RATING_COLLECTION )

    // 如果表已经存在，则删掉
    productCollection.dropCollection()
    ratingCollection.dropCollection()

    // 将当前数据存入对应的表中
    productDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", GlobalConstant.MONGODB_PRODUCT_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", GlobalConstant.MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // 对表创建索引
    productCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "productId" -> 1 ) )
    ratingCollection.createIndex( MongoDBObject( "userId" -> 1 ) )

    mongoClient.close()
  }

  def loadRatingDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig): DataFrame ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()
  }

  def loadProductDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig): DataFrame ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Product]
      .toDF()
  }

  def insertDFInMongoDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit ={
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }

  def loadMovieRecDFInMongoDB(spark:SparkSession, collection_name: String)(implicit mongoConfig: MongoConfig):collection.Map[Int, Map[Int, Double]] ={
    import spark.implicits._
    spark.read
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .format("com.mongodb.spark.sql")
      .load()
      .as[ProductRecs]
      .rdd
      .map{ movieRecs => // 为了查询相似度方便，转换成map
        (movieRecs.productId, movieRecs.recs.map( x=> (x.productId, x.score) ).toMap )
      }.collectAsMap()
  }

  // 写入mongodb
  def saveDataToMongoDB(userId: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig: MongoConfig): Unit = {
    // 定义到StreamRecs表的连接
    val streamRecsCollection = MongoClient(MongoClientURI(mongoConfig.uri))(mongoConfig.db)(GlobalConstant.STREAM_RECS)
    // 按照userId查询并更新
    streamRecsCollection.findAndRemove(MongoDBObject("userId" -> userId))
    streamRecsCollection.insert(MongoDBObject("userId" -> userId,
      "recs" -> streamRecs.map(x => MongoDBObject("productId" -> x._1, "score" -> x._2))))
  }

}
