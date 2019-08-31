package Utils




import java.text.SimpleDateFormat


object CalculateTools {

  def getDate(requestId:String,endTime:String)={
//    val startTime = requestId.substring(0,17)
//    //val format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")
//
//    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
//    format.parse(endTime).getTime - format.parse(startTime).getTime
      val df = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    // 20170412030013393282687799171031
    // 开始时间
      val st: Long = df.parse(requestId.substring(0,17)).getTime
    // 结束时间
      val et = df.parse(endTime).getTime
       et-st
  }

}