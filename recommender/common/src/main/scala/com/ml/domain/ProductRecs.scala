package com.ml.domain
/**
  * Project: ECSystem
  * ClassName: com.ml.domain.ProductRecs
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 下午 12:28
  */
// 定义商品相似度列表
case class ProductRecs( productId: Int, recs: Seq[Recommendation] )
