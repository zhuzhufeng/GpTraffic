package Context

import java.lang

import Utils.CalculateTools

import Utils.{CalculateTools, Jedis2Result}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Redis管理Offset
  * 实时获取数据--并对数据进行处理
  */
object KafkaRedisOffset {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("offset").setMaster("local[2]")
      // 设置没秒钟每个分区拉取kafka的速率
      .set("spark.streaming.kafka.maxRatePerPartition","100")
      // 设置序列化机制
      .set("spark.serlizer","org.apache.spark.serializer.KryoSerializer")
    val ssc = new StreamingContext(conf,Seconds(1))
    // 配置参数
    // 配置基本参数
    // 组名
    val groupId = "zk002"
    // topic
    val topic = "zf01"
    // 指定Kafka的broker地址（SparkStreaming程序消费过程中，需要和Kafka的分区对应）
    val brokerList = "192.168.186.110:9092"
    // 编写Kafka的配置参数
    val kafkas = Map[String,Object](
      "bootstrap.servers"->brokerList,
      // kafka的Key和values解码方式
      "key.deserializer"-> classOf[StringDeserializer],
      "value.deserializer"-> classOf[StringDeserializer],
      "group.id"->groupId,
      // 从头消费
      "auto.offset.reset"-> "earliest",
      // 不需要程序自动提交Offset
      "enable.auto.commit"-> (false:lang.Boolean)
    )
    // 创建topic集合，可能会消费多个Topic
    val topics = Set(topic)
    // 第一步获取Offset
    // 第二步通过Offset获取Kafka数据
    // 第三步提交更新Offset
    // 获取Offset
    var fromOffset:Map[TopicPartition,Long] = JedisOffset(groupId)
    // 判断一下有没数据--》
    val stream :InputDStream[ConsumerRecord[String,String]] =
      if(fromOffset.size == 0){
        KafkaUtils.createDirectStream(ssc,
          // 本地策略
          // 将数据均匀的分配到各个Executor上面
          LocationStrategies.PreferConsistent,
          // 消费者策略
          // 可以动态增加分区
          ConsumerStrategies.Subscribe[String,String](topics,kafkas)
        )
      }else{
        // 不是第一次消费
        KafkaUtils.createDirectStream(
          ssc,
          LocationStrategies.PreferConsistent,
          ConsumerStrategies.Assign[String,String](fromOffset.keys,kafkas,fromOffset)
        )
      }
    //将省份表广播出去
    val map = ssc.sparkContext.textFile("src/main/dir/city.txt").map(t=>(t.split(" ")(0),t.split(" ")(1))).collect.toMap
    val broad = ssc.sparkContext.broadcast(map)
      println("111111111111111111111111")
      stream.foreachRDD({
         rdd=>
         val offestRange = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
           // 业务处理-->rdd.map(_.value())是获取实时的数据的固定方法
           println("1*************************")

           rdd.map(_.value()).foreach(println) //-->实时打印数据
           //map(t=>JSON.parseObject(t))-->将数据类型转换才能处理 -->转换后数据类型RDD[JSONObject]
              val baseRDD: RDD[(String , String ,String , (String ,String),List[Double],String)] = rdd.map(_.value()).map(t => JSON.parseObject(t))
                //过滤符合条件的接口，判断接口调用是否成功
                .filter(json => json.getString("serviceName").equalsIgnoreCase("reChargeNotifyReq") &&
                json.getString("interFacRst").equals("0000"))
                .map(t => {
                  //依次取出所需要的值-->充值结果
                  val result = t.getString("bussinessRst") // 充值结果
                  val money:Double = if(result.equals("0000")) t.getDouble("chargefee") else 0.0 // 充值金额
                  //成功数
                  val success = if(result.equals("0000"))  1 else 0 // 充值成功数
                  //获取省份编码
                  val pcode = t.getString("provinceCode")
                  val provience: String = broad.value.get(pcode).get
                  //读取开始充值时间 20170412030019969019312508942847 需要对字符串截取
                  val requestId = t.getString("requestId")     //读取结束充值时间 20170412030046608
                  //获取日期
                  val data = requestId.substring(0, 8)
                  //小时
                  val hour = requestId.substring(8, 10)
                  //分钟
                  val minute = requestId.substring(10, 12)
                  //充值结束时间
                  val receiveTime = t.getString("receiveNotifyTime")
                  val time: Long = CalculateTools.getDate(requestId,receiveTime)
                  (data, minute,hour,(hour,provience),List(1,money, success,time),provience)
                  //redis.expire("A-" + tp._1, 60 * 60 * 48)
                }).cache()
           //指标1-->统计全网的充值订单量, 充值金额, 充值成功数
          // baseRDD

           Jedis2Result.Result01(baseRDD.map(t=>(t._1,t._5)))

           //指标一 2 实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
          Jedis2Result.Result012(baseRDD.map(t=>(t._2,t._5.head)).reduceByKey(_+_))


           //指标2-->统计每小时各个省份的充值失败数据量
//          Jedis2Result.Result02(baseRDD.map(t=>(t._4,t._5)).reduceByKey((list1,list2)=>{
//             // list1(1,2,3).zip(list2(1,2,3)) = list((1,1),(2,2),(3,3))
//             // map处理内部的元素相加
//           list1.zip(list2).map(t=>t._1+t._2)
//           }))

           /**
             *以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，
             * 只保留一位小数，存入MySQL中，进行前台页面显示。
             * 需要的数据:省份 订单
             */
           val result4 = baseRDD.map(t=>(t._6,t._5)).reduceByKey((list1,list2)=>{list1.zip(list2).map(t=>t._1+t._2)})
          // Jedis2Result.Result03(result4,ssc)
           /**
               实时统计每小时的充值笔数和充值金额。
             */
//           Jedis2Result.Result04(baseRDD.map(t=>(t._3,List(t._5.head,t._5(1)))).reduceByKey((list1,list2)=>{
//             list1.zip(list2).map(tp =>tp._1+tp._2)
//           })

           val res: RDD[(String, List[Double])] = baseRDD.map(tp => (tp._3, List(tp._5.head, tp._5(1)))).reduceByKey((list1, list2) => {
             list1.zip(list2).map(tp => tp._1 + tp._2)
           })

           //Jedis2Result.Result04(res)




           // 将偏移量进行更新
        val jedis = JedisConnectionPool.getConnection()
        for (or<-offestRange){
          jedis.hset(groupId,or.topic+"-"+or.partition,or.untilOffset.toString)
        }
        jedis.close()
    })
    // 启动
    ssc.start()
    ssc.awaitTermination()
  }
}
