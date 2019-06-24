package com.ml.domain

/**
  * Project: ECSystem
  * ClassName: com.ml.domain.MongoConfig
  * Version: V1.0
  *
  * Author: LTong
  * Date: 2019-06-24 上午 11:28

  * MongoDB连接配置
  * @param uri    MongoDB的连接uri
  * @param db     要操作的db
  */
case class MongoConfig( uri: String, db: String )
