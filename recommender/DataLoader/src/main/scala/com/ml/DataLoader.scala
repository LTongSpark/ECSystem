package com.ml

import com.ml.common.GlobalConstant
import com.ml.conf.SparkSess
import com.ml.domain.{ Product, Rating}
import com.ml.util.MongoUtil

/**
  * Project: ECSystem
  * ClassName: com.ml.DataLoader
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 9:42
  */

object DataLoader extends SparkSess{
  def main(args: Array[String]): Unit = {
    import spark.implicits._
    // 加载数据
    val productRDD = spark.sparkContext.textFile(GlobalConstant.PRODUCT_DATA_PATH)
    val productDF = productRDD.map( item => {
      // product数据通过^分隔，切分出来
      val attr = item.split("\\^")
      // 转换成Product
      Product( attr(0).toInt, attr(1).trim, attr(4).trim, attr(5).trim, attr(6).trim )
    } ).toDF()

    val ratingRDD = spark.sparkContext.textFile(GlobalConstant.RATING_DATA_PATH)
    val ratingDF = ratingRDD.map( item => {
      val attr = item.split(",")
      Rating( attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt )
    } ).toDF()
    //保存数据到mongodb中
    MongoUtil.storeDataInMongoDB( productDF, ratingDF )
    spark.stop()
  }

}
