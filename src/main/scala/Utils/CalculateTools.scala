package Utils



import java.text.SimpleDateFormat

object CalculateTools {

  def getDate(requestId:String,endTime:String)={
    val startTime = requestId.substring(0,17)
    //val format = FastDateFormat.getInstance("yyyyMMddHHmmssSSS")

    val format = new SimpleDateFormat("yyyyMMddHHmmssSSS")
    format.parse(endTime).getTime - format.parse(startTime).getTime

  }

}