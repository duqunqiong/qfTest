package Request06_Tags
import log_Class.log
import org.apache.spark.rdd.RDD

/**
  *
  * @description 地域关键字
  * @author duqunqiong
  * @date 2020/6/17 20:09
  *
  */

class Tags_LocationPlaces extends Tags {
  override def creatTag(table: RDD[log]): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    table.foreach(t=>{
      list:+=("ZP"+t.provincename,1)
      list:+=("ZC"+t.cityname,1)
    })

    list
  }
}
