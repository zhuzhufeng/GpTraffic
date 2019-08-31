package Utils

import java.text.SimpleDateFormat
import java.util.Date

/**
  * 工具类
  */
object Utils_Time {
    //时间工具类
  def costtime(startTime:String,stopTime:String): Long ={
    val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")

    // 20170412030013393282687799171031
    // 开始时间
val st: Long = df.parse(startTime.substring(0,17)).getTime
  //节点胡时间
    val et = df.parse(stopTime).getTime
    et-st
  }

}
