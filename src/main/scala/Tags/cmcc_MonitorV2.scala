//package Tags
//
//
//import Utils.{ApiUtils, AppParameters, OffsetManager}
//import com.alibaba.fastjson.JSON
//import org.apache.kafka.clients.consumer.ConsumerRecord
//import org.apache.kafka.common.TopicPartition
//import org.apache.log4j.{Level, Logger}
//import org.apache.spark.SparkConf
//import org.apache.spark.broadcast.Broadcast
//import org.apache.spark.rdd.RDD
//import org.apache.spark.streaming.kafka010._
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object cmcc_MonitorV2 {
//
//  def main(args: Array[String]): Unit = {
//    //取消日志显示
//    Logger.getLogger("org.apache.spark").setLevel(Level.OFF)
//    //SparkCOnf属性配置
//    val conf = new SparkConf().setAppName("中国移动实时监控平台_V2").setMaster("local[*]")
//    //RDD序列化 节约内存
//    conf.set("spark.serialize","org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.streaming.kafka.maxRatePerPartition","10000")   //拉取数据
//    conf.set("spark.streaming.kafka.stopGracefullyOnShutdown","true")  //优雅的停止
//
//    //创建SparkStreaming
//    val ssc = new StreamingContext(conf,Seconds(2))
//    /**
//      * 提取数据库中的存储的偏移量
//      */
//    val currOffser: Map[TopicPartition, Long] = OffsetManager.getMyCurrentOffset
//
//
//    //使用广播的方式匹配省份
//    val provinceName: Broadcast[Map[String, AnyRef]] = ssc.sparkContext.broadcast(AppParameters.provinces)
//
//    //创建直接从kafka中读取数据的对象
//    val stream = KafkaUtils.createDirectStream(ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String ,String ](AppParameters.topic,AppParameters.kafkaParams,currOffser))
//
//    /**
//      * 开始计算
//      */
//    stream.foreachRDD(baseRdd =>{
//
//      val offsetRanges: Array[OffsetRange] = baseRdd.asInstanceOf[HasOffsetRanges].offsetRanges
//
//      val baseData = ApiUtils.Api_BaseDate(baseRdd)
//
//
//
//
//      /**
//        * 计算每日的业务概况
//        */
//      ApiUtils.Api_general_total(baseData)
//
//      /**
//        * 计算实时充值办理业务趋势
//        */
//      ApiUtils.api_general_hour(baseData)
//
//      /**
//        * 计算全国各省充值业务失败量分布
//        */
//      ApiUtils.api_general_province(baseData,provinceName)
//      /**
//        * 实时统计每分钟的充值金额和订单量
//        */
//      ApiUtils.api_realtime_minute(baseData)
//
//      /**
//        * 存储偏移量
//        */
//      OffsetManager.saveCurrentOffset(offsetRanges)
//
//
//
//    })
//
//    ssc.start()
//
//    ssc.awaitTermination()
//
//  }
//
//
//
//}