package com.ml
import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.{ProductRecs, Recommendation}
import com.ml.util.MongoUtil
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.ml.linalg.SparseVector
import org.jblas.DoubleMatrix
/**
  * Project: ECSystem
  * ClassName: com.ml.ContentRecommender
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 10:03
  */

object ContentRecommender extends SparkSess{
  def main(args: Array[String]): Unit = {
    import spark.implicits._

    val productTagsDF = product.rdd.map(x =>(x.getAs[Int]("productId") ,x.getAs[String]("name") ,
      x.getAs[String]("tags"))).map{
      case(mid ,name ,geners) =>{
        (mid,name ,geners.split("\\|").mkString(" "))
      }
    }.toDF("productId", "name", "tags").cache()

    // TODO: 用TF-IDF提取商品特征向量
    // 1. 实例化一个分词器，用来做分词，默认按照空格分
    val tokenizer = new Tokenizer().setInputCol("tags").setOutputCol("words")
    // 用分词器做转换，得到增加一个新列words的DF
    val wordsDataDF = tokenizer.transform(productTagsDF)

    // 2. 定义一个HashingTF工具，计算频次
    val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(800)
    val featurizedDataDF = hashingTF.transform(wordsDataDF)

    // 3. 定义一个IDF工具，计算TF-IDF
    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    // 训练一个idf模型
    val idfModel = idf.fit(featurizedDataDF)
    // 得到增加新列features的DF
    val rescaledDataDF = idfModel.transform(featurizedDataDF)

    // 对数据进行转换，得到RDD形式的features
    val productFeatures = rescaledDataDF.map{
      row => ( row.getAs[Int]("productId"), row.getAs[SparseVector]("features").toArray )
    }
      .rdd
      .map{
        case (productId, features) => ( productId, new DoubleMatrix(features) )
      }

    // 两两配对商品，计算余弦相似度
    val productRecs = productFeatures.cartesian(productFeatures)
      .filter{
        case (a, b) => a._1 != b._1
      }
      // 计算余弦相似度
      .map{
      case (a, b) =>
        val simScore = consinSim( a._2, b._2 )
        ( a._1, ( b._1, simScore ) )
    }
      .filter(_._2._2 > 0.4)
      .groupByKey()
      .map{
        case (productId, recs) =>
          ProductRecs( productId, recs.toList.sortWith(_._2>_._2).map(x=>Recommendation(x._1,x._2)) )
      }
      .toDF()
    //将数据保存在mongodb中
    MongoUtil.insertDFInMongoDB(productRecs ,GlobalConstant.PRODUCT_RECS)

    spark.stop()
  }
  def consinSim(product1: DoubleMatrix, product2: DoubleMatrix): Double ={
    product1.dot(product2)/ ( product1.norm2() * product2.norm2() )
  }
}

