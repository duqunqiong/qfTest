package Request06_Tags
import log_Class.log
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.util.StringUtils

/**
  *
  * @description 广告位 ，广告类型
  * @author duqunqiong
  * @date 2020/6/17 19:31
  *
  */

class Tags_LC extends Tags {
  override def creatTag(table: RDD[log]): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    table.foreach(t =>{
      val advType: Int = t.adspacetype
      advType match {
        case v if (v <10 && v >0) => list:+=("LC" + advType,1)
        case v if v > 10 => list:+=("LC0"+advType,1)
      }

      val name: String = t.adspacetypename
      if(!name.isEmpty())
        list:+=("LN"+ name,1)
    })

    list
  }
}
