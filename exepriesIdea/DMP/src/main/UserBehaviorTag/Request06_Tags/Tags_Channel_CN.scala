package Request06_Tags

import log_Class.log
import org.apache.spark.rdd.RDD

/**
  *
  * @description 创建渠道标签
  * @author duqunqiong
  * @date 2020/6/17 15:47
  *
  */

class Tags_Channel_CN extends Tags {


  def creatTag(table: RDD[log]): List[(String, Int)] = {

    var list = List[(String,Int)]()

    table.foreach(t => {
      val channelName: String = "CN" + t.adplatformproviderid
      list:+= (channelName,1)
    })

    list

  }
}
