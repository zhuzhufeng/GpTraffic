package TestContext

import Context.JedisConnectionPool
import Utils.ConnectPoolUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

/**
  * 实现业务的功能需求的具体方法
  *
  */
object JedisAPP {
  //指标1
  def Result01(lines:RDD[(String,List[Double])]): Unit ={
    lines.foreachPartition(f=>{
      //拿到Jdeis的连接 方便存入redis
      val jedis = JedisConnectionPool.getConnection()
      //对每一分区的数据进行计算
      f.foreach(t=>{
        //充值订单数
        jedis.hincrBy(t._1,"count",t._2.head.toLong)
        //充值金额
        jedis.hincrBy(t._1,"money",t._2(1).toLong)
        jedis.hincrBy(t._1,"success",t._2(2)toLong)
        jedis.hincrBy(t._1,"time",t._2(3).toLong)
      })
      jedis.close()
    })

  }

  def Result011(lines:RDD[(String ,Double)])= {
    lines.foreachPartition(f => {
      //拿到Jdeis的连接 方便存入redis
      val jedis = JedisConnectionPool.getConnection()
      //对每一分区的数据进行计算
      f.foreach(t => {
        jedis.incrBy(t._1, t._2.toLong)
      })
      jedis.close()
    })
  }
  // 指标二
  def Result02(lines:RDD[((String, String), List[Double])]): Unit ={
    lines.foreachPartition(f=> {
      val conn = ConnectPoolUtils.getConnections()
  f.foreach(t=>{
   val sql= "insert into ProHour(pro,Hour,Counts)" +
      "values('"+t._1._1+"','"+t._1._2+"',"+(t._2(0)-t._2(2))+""


    val state = conn.createStatement()

      state.executeUpdate(sql) })

    //还连接
    ConnectPoolUtils.resultConn(conn)
   })
  }

  //指标3
  def Result03(lines:RDD[(String, List[Double])],ssc:StreamingContext): Unit ={
    //先进性排序,按照订单量进行排序
    val sortKV = lines.sortBy(_._2.head,false)
    //进行百分比求职(成功lv )
    val value = sortKV.map(t => (t._1, (t._2(2) / t._2.head).toInt,t._2(0).toInt)).take(10)
    // 创建新的RDD
    val rdd = ssc.sparkContext.makeRDD(value)
    // 循环没个分区
    rdd.foreachPartition(t=>{
      //拿到连接
      val connection = ConnectPoolUtils.getConnections()
    t.foreach(t=>{
      val sql = "insert into tb_access_status(logDate,pv,uv)" +
        " values('"+t._1+"','"+t._3+"','"+t._2+"')"
      val state = connection.createStatement()
      state.executeUpdate(sql)

    })

      ConnectPoolUtils.resultConn(connection)

    })


  }


}
