package Request06_Tags

import log_Class.{UserLog, log}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * @description 上下文标签，用于合并总标签
  * @author duqunqiong
  * @date 2020/6/17 15:00
  *
  */

object Request06_01Tags {
  def main(args: Array[String]): Unit = {
    // 得到参数
    val log: (RDD[log], SparkSession) = new UserLog().creatUserLog
    val logUser: RDD[log] = log._1
    val sparkSession: SparkSession = log._2

    // 创建各个标签
    val APP            = new Tags_APP().creatTag(logUser)
    val Channel_CN     = new Tags_Channel_CN().creatTag(logUser)
    val Device         = new Tags_Device().creatTag(logUser)
    val KeyWords       = new Tags_KeyWords().creatTag(logUser)
    val LC             = new Tags_LC().creatTag(logUser)
    val LocationPlaces = new Tags_LocationPlaces().creatTag(logUser)

    // 标签合并


  }
}
