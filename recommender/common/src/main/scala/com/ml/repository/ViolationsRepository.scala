package com.ml.repository

import org.apache.kafka.common.serialization.StringDeserializer

/**
  * @author LTong
  * @date 2019-06-18 下午 1:01
  */
object ViolationsRepository {

  val mongoConfig:Map[String ,String] = Map(
    "mongo.uri" -> "mongodb://localhost:27017/ECrecommender",
    "mongo.db" -> "ECrecommender"

  )
  val kafkaConfig = Map(
    "kafka.topic" -> "ECrecommender"
  )
  val kafkaParam = Map(
    "bootstrap.servers" -> "zytc222:9092,zytc223:9092,zytc224:9092",
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> "ECrecommender",
    "auto.offset.reset" -> "latest"
  )

  val jedisConfig =Map(
    "jedis.host" ->"localhost"
  )

}
