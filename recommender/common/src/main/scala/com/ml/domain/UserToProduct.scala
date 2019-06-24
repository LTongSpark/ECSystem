package com.ml.domain

/**
  * @author LTong
  * @date 2019-06-18 下午 3:34
  *      定义基于预测评分的用户推荐列表
  */
case class UserToProduct(userId: Int, recs: Seq[Recommendation])
