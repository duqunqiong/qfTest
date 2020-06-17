package Request06_Tags

import log_Class.log
import org.apache.spark.rdd.RDD
/**
  *
  * @description 创建APP标签
  * @author duqunqiong
  * @date 2020/6/17 15:43
  *
  */

class Tags_APP extends Tags {
  // 判断是否存在

  def creatTag(table: RDD[log]): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    table.foreach(t =>{
     val APPName: String = "APP"+ t.appname
      list:+=(APPName,1)
    })

    list
  }

}
