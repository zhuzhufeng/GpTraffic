package Utils


import com.typesafe.config.ConfigFactory
import org.apache.kafka.common.serialization.StringDeserializer

object AppParameters {

  /**
    * kafka配置设置
    */
  //读取配置文件  读取顺序：application.conf-->application.json-->application.properties
  val config = ConfigFactory.load()

  val topic: Array[String] = config.getString("kafka.topic").split(",")
  val groupId = config.getString("kafka.groupId")
  val brokers = config.getString("kafka.broker")

  val kafkaParams = Map[String, Object](
    "bootstrap.servers" -> brokers,
    "key.deserializer" -> classOf[StringDeserializer],
    "value.deserializer" -> classOf[StringDeserializer],
    "group.id" -> groupId,
    "auto.offset.reset" -> "earliest",
    "enable.auto.commit" -> (false: java.lang.Boolean)
  )

  /**
    * Redis设置
    * 主机 和 第几个数据库数据库
    */
  val redis_host = config.getString("redis.host")

  val redis_index = config.getString("redis.index").toInt

  /**
    * 省份设置
    */
  import scala.collection.JavaConversions._
  val provinces = config.getObject("province").unwrapped().toMap
}