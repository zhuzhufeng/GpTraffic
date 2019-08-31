package Utils

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * 业务结果调用接口
  */
object Jedis2Result {
  /**
    * 指标1
    * 1)统计全网的充值订单量, 充值金额, 充值成功数
    */
  def  Result01(lines:RDD[(String , List[Double])] )={
    lines.foreachPartition(f=>{
      //获取iedis的连接
      val jedis = JedisConnectionPool.getConnection()

      f.foreach(t=>{
        //充值总数
        jedis.hincrBy(t._1,"total",t._2(0).toLong)
        //充值金额
        jedis.hincrByFloat(t._1,"money",t._2(1))
        //充值成功数
        jedis.hincrBy(t._1,"success",t._2(2).toLong)
        //充值时长
        jedis.hincrBy(t._1,"time",t._2(3).toLong)
      })
      jedis.close()
    })
  }



  /**
    * 统计每小时各个省份的充值失败数据量
    */
  def Result02(lines:RDD[((String,String ),List[Double])]): Unit ={
     lines.map(t=>{
       val protime=t._1
       //成功的减去失败的
       val count=t._2.head-t._2(2)
       (protime,count)
     })
       .foreachPartition(f=>{
       //创建JDBC Connection连接
       val connection = ConnectPoolUtils.getConnections()
      f.foreach(t=>{
        val sql="insert  into ProTime(protime,count) values('"+t._1+"','"+t._2+"')"
      //加载语句
        val statement = connection.createStatement()
        statement.executeUpdate(sql)
      })
         ConnectPoolUtils.resultConn(connection)
     })
  }


//以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。以省份为维度统计订单量排名前 10 的省份数据,并且统计每个省份的订单成功率，只保留一位小数，存入MySQL中，进行前台页面显示。
  def Result03(lines:RDD[(String ,List[Double])],ssc:StreamingContext): Unit ={
    // 先进行排序，安装订单量进行排序
    val sortKV = lines.sortBy(_._2.head,false)
    // 进行百分比求值（成功率） 省份,订单率,订单数
    val value: Array[(String, Int, Int)] = sortKV.map(t => (t._1, (t._2(2) / t._2.head).toInt,t._2(0).toInt)).take(10)
    //创建新的RDD
    val rdd = ssc.sparkContext.makeRDD(value)
    //循环每个分区
    rdd.foreachPartition(f=>{
      //拿到连接
      val connection = ConnectPoolUtils.getConnections()
      //将数据存储到mysql中
      // 将数据存储到Mysql
      f.foreach(t=>{
        val sql = "insert into realsucess(province,pv,uv)" +
          " values('"+t._1+"','"+t._3+"','"+t._2+"')"
        val state = connection.createStatement()
        state.executeUpdate(sql)
      })
      //归还连接池
      ConnectPoolUtils.resultConn(connection )

    })

  }


  //实时充值业务办理趋势, 主要统计全网每分钟的订单量数据
  def Result012(lines:  RDD[(String ,Double)]): Unit ={
    lines.foreachPartition(f=>{
      val jedis = JedisConnectionPool.getConnection()
      f.foreach(t=>{
        //充值总数
        //jedis.hincrBy("D-"+t._1._1,"Num-"+t._1._2+t._1._3,t._2(0).toLong)
        jedis.hincrBy("Minutre",t._1,t._2.toLong)


      })
      jedis.close()
    })

  }


  //实时统计每小时的充值笔数和充值金额 -->实时充值情况分布（存入MySQL）
  def Result04(lines:  RDD[(String, List[Double])]): Unit ={
    lines.foreachPartition(f=>{
      //拿到连接
      val connection = ConnectPoolUtils.getConnections()
      //将数据存储到mysql中
      f.foreach(t=>{

        val sql="insert into table realHour(logHour,pv,uv)" +
          "values('"+t._1+"','"+t._2.head+"','"+t._2(1)+"')"
        //加载语句
        val state = connection.createStatement()
        //更新
        state.executeUpdate(sql)
      })
      //归还连接池
      ConnectPoolUtils.resultConn(connection )



    })

  }
}
