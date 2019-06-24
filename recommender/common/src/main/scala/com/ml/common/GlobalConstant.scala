package com.ml.common

/**
  * Project: ECSystem
  * ClassName: com.ml.common.GlobalConstant
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 10:47
  */
object GlobalConstant {
  // 定义数据文件路径
  val PRODUCT_DATA_PATH = "D:\\idea\\ECSystem\\recommender\\DataLoader\\src\\main\\resources\\products.csv"
  val RATING_DATA_PATH = "D:\\idea\\ECSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  // 定义mongodb中存储的表名
  val MONGODB_PRODUCT_COLLECTION = "Product"
  val MONGODB_RATING_COLLECTION = "Rating"
  val RATE_MORE_PRODUCTS = "RateMoreProducts"
  val RATE_MORE_RECENTLY_PRODUCTS = "RateMoreRecentlyProducts"
  val AVERAGE_PRODUCTS = "AverageProducts"
  val USER_TO_PRODUCT = "UserToProduct"
  val PRODUCT_TO_USER = "ProductToUser"

  val STREAM_RECS = "StreamRecs"
  val PRODUCT_RECS = "ProductRecs"

  val ITEM_CF_PRODUCT_RECS = "ItemCFProductRecs"
  val MAX_RECOMMENDATION = 10

  val MAX_USER_RATING_NUM = 20
  val MAX_SIM_PRODUCTS_NUM = 20

}
