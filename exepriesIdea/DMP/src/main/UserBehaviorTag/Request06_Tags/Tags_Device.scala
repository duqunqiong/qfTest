package Request06_Tags
import log_Class.log
import org.apache.spark.rdd.RDD

/**
  *
  * @description
  * @author duqunqiong
  * @date 2020/6/17 19:55
  *
  */

class Tags_Device extends Tags {
  override def creatTag(table: RDD[log]): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    table.foreach(t =>{
      // 设备操作系统
      t.client match{
        case v if (v == 1) => list:+=("D00010001",1)
        case v if (v == 2) => list:+=("D00010002",1)
        case v if (v == 3) => list:+=("D00010003",1)
        case _ => list:+=("D00010004",1)
      }

      //设备联网方式
      t.networkmannername match {
        case v if ("WIFI".equals(v)) => list:+=("D00020001",1)
        case v if ("4G".equals(v)) => list:+=("D00020002",1)
        case v if ("3G".equals(v)) => list:+=("D00020003",1)
        case v if ("2G".equals(v)) => list:+=("D00020004",1)
        case _ => list:+=("D00020004",1)
      }

      // 设备运营商
      t.ispname match {
        case v if ("移动".equals(v)) => list:+=("D00030001",1)
        case v if ("联通".equals(v)) => list:+=("D00030002",1)
        case v if ("电信".equals(v)) => list:+=("D00030003",1)
        case _ => list:+=("D00030004",1)
      }
    })
list
  }
}
