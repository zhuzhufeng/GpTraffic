//package Utils
//
//
//import org.apache.kafka.common.TopicPartition
//import org.apache.spark.streaming.kafka010.OffsetRange
//import org.iq80.leveldb.DB
//
//object OffsetManager {
//  //加载配置文件  application.conf
//  DBs.setup()
//
//  /**
//    * 获取自己存储的偏移量信息
//    * @return
//    */
//  def getMyCurrentOffset :Map[TopicPartition,Long] = {
//    DB.readOnly(implicit session =>
//      SQL("select * from streaming_offset where groupId  = ?").bind(AppParameters.groupId)
//        .map(rs =>
//          (
//            new TopicPartition(rs.string("topicName"),rs.int("partitionId")),
//            rs.long("offset")
//          )
//        ).list().apply().toMap
//    )
//
//
//  }
//
//  /**
//    * 持久化存储当前的偏移量
//    */
//  def saveCurrentOffset(offsetRanges: Array[OffsetRange]): Unit ={
//
//    DB.localTx(implicit session =>{
//      offsetRanges.foreach(or =>{
//        SQL("replace into streaming_offset values(?,?,?,?)")
//          .bind(or.topic,or.partition,or.untilOffset,AppParameters.groupId)
//          .update()
//          .apply()
//      })
//    })
///**/
//
//  }
//
//
//}}