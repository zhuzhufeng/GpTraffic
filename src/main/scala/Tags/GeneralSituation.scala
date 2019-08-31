package Tags

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 离线分析
  */


object GeneralSituation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("general").setMaster("local[2]")
    val sc = new SparkContext(conf)
    //val sQLContext = new  SQLContext(sc)
    val logs = sc.textFile("src/main/dir/cmcc.json").collect().toBuffer
    var buffer = List[(Int,Double,Int)]()
    var fee=0.0
    var succPay = 0
    var cnt =0

    for(i <- logs){
      val nObject = JSON.parseObject(i)
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

    }
    print(cnt+" "+fee+" "+succPay)



  }
}