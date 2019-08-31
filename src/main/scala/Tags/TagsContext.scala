package Tags

import org.apache.spark
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  *
  */
object TagsContext {
  def main(args: Array[String]): Unit = {
    //初始化
    val spark = SparkSession.builder().appName(this.getClass.getName).master("local[*]").getOrCreate()
    //读取数据
    val frame = spark.read.json("D:/Traffic/src/main/dir/cmcc.json")
    frame.createOrReplaceTempView("data")
    //1)统计全网的充值订单量, 充值金额, 充值成功数
    spark.sql(" select  count(orderId),sum(chargefee), count(bussinessRst) from data  where bussinessRst=0000").show()

    spark.stop()
  }



}
