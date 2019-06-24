package com.ml
import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.{ProductRecs, Recommendation}
import com.ml.util.MongoUtil
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Project: ECSystem
  * ClassName: com.ml.ItemCFRecommender
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 10:05
  */

object ItemCFRecommender extends SparkSess{

  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val ratingDF = rating.rdd.map(x =>(x.getAs[Int]("userId") ,x.getAs[Int]("productId") ,
      x.getAs[Double]("score"))).toDF("userId", "productId", "score").cache()


    // TODO: 核心算法，计算同现相似度，得到商品的相似列表
    // 统计每个商品的评分个数，按照productId来做group by
    val productRatingCountDF = ratingDF.groupBy("productId").count()
    // 在原有的评分表上rating添加count
    val ratingWithCountDF = ratingDF.join(productRatingCountDF, "productId")

    // 将评分按照用户id两两配对，统计两个商品被同一个用户评分过的次数
    val joinedDF = ratingWithCountDF.join(ratingWithCountDF, "userId")
      .toDF("userId","product1","score1","count1","product2","score2","count2")
      .select("userId","product1","count1","product2","count2")

    import org.apache.spark.sql.functions._
    val cooccurrenceDF = joinedDF.groupBy("product1","product2")
      .agg(count("userId").as("userid_count") ,first("count1")as("count1") ,
        first("count2").as("count2"))
      .select("product1","product2","userid_count" ,"count1","count2")

    // 提取需要的数据，包装成( productId1, (productId2, score) )
    val simDF = cooccurrenceDF.map{
      row =>
        val coocSim = cooccurrenceSim( row.getAs[Long]("userid_count"),
          row.getAs[Long]("count1"), row.getAs[Long]("count2") )
        ( row.getInt(0), ( row.getInt(1), coocSim ) )
    }
      .rdd
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs( productId, recs.toList
            .filter(x=>x._1 != productId)
            .sortBy(-_._2)
            .take(GlobalConstant.MAX_RECOMMENDATION)
            .map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()

    MongoUtil.insertDFInMongoDB(simDF,GlobalConstant.ITEM_CF_PRODUCT_RECS)

    spark.stop()
  }

  // 按照公式计算同现相似度
  def cooccurrenceSim(coCount: Long, count1: Long, count2: Long): Double ={
    coCount / math.sqrt( count1 * count2 )
  }
}

