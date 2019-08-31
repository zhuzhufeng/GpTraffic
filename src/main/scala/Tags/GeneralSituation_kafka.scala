package Tags



import java.util.{Collections, Properties}

import Utils.JedisConnectionPool
import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}



object GeneralSituation_kafka {
  def main(args: Array[String]): Unit = {
    val prop = new Properties
    prop.put("bootstrap.servers", "hadoop01:9092")
    prop.put("group.id", "group01")
    //消费位置
    prop.put("auto.offset.reset", "latest")
    //key的反序列化方式
    prop.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    prop.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    var fee = 0.0
    var succPay = 0
    var cnt = 0
    val consumer = new KafkaConsumer[String, String](prop)
    consumer.subscribe(Collections.singletonList("traffic"))
    while(true){
      //消费到数据
      val msgs: ConsumerRecords[String, String] = consumer.poll(5000)
      println(msgs.iterator().next().value())


      //获得生产的数据
      while(msgs.iterator().hasNext){
        val nObject: JSONObject = JSON.parseObject(msgs.iterator().next().value())
        val result = nObject.getString("bussinessRst")
        if(result.equals("0000")){
          fee += nObject.getDouble("chargefee")
          succPay +=1
        }else {
          fee += 0
          succPay +=0
        }
        cnt+=1
        (cnt,fee,succPay)
        println(cnt+" "+fee+" "+succPay)

        /////////////////////////////////////////////////////////////////////////////////

      }


      //

    }
  }

}
