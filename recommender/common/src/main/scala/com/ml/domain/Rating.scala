package com.ml.domain

/**
  * Project: ECSystem
  * ClassName: com.ml.domain.Rating
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 11:27

  * Rating数据集
  * 4867        用户ID
  * 457976      商品ID
  * 5.0         评分
  * 1395676800  时间戳
  */
case class Rating( userId: Int, productId: Int, score: Double, timestamp: Int )
