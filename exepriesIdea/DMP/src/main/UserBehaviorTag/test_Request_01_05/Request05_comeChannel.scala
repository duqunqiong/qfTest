package test_Request_01_05

import log_Class.{UserLog, log}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @description 用户来源的渠道
  * @author duqunqiong
  * @date 2020/6/17 13:12
  *
  */

object Request05_comeChannel {
  def main(args: Array[String]): Unit = {
    // 得到参数
    val log: (RDD[log], SparkSession) = new UserLog().creatUserLog
    val logUser: RDD[log] = log._1
    val sparkSession: SparkSession = log._2

    // 隐式转换

    val userLog: RDD[log] = logUser.coalesce(1)
    import sparkSession.implicits._
    val userLogDF: DataFrame = userLog.toDF()
    // userLogDF.show()

    userLogDF.createTempView("userLog")

    // val dataDF: DataFrame = sparkSession.read.text("data\\textLog.log")
    val sql04: DataFrame = sparkSession.sql(
      """ select
        |channelid,
        |sum(case when requestmode == 1 and processnode >= 1 then 1 else 0 end) orgRequest,
        |sum(case when requestmode == 1 and processnode >= 2 then 1 else 0 end) infRequest,
        |sum(case when requestmode == 1 and processnode == 3 then 1 else 0 end) advRequest,
        |sum(case when isbilling == 1 and isbid == 1 and iseffective == 1 then 1 else 0 end) bidNum,
        |sum(case when isbilling == 1 and iswin == 1 and iseffective == 1 and adorderid != 0 then 1 else 0 end) winBidNum,
        |sum(case when requestmode == 2 and iseffective == 1 then 1 else 0 end) showNum,
        |sum(case when requestmode == 3 and iseffective == 1 then 1 else 0 end) clickNum,
        |sum(case when isbilling == 1 and iseffective == 1 and iswin == 1 then winprice/1000 else 0 end)  winPrice,
        |sum(case when isbilling == 1 and iseffective == 1 and iswin == 1 then adppprice/1000 else 0 end) adppprice
        |from userLog
        |group by channelid
      """.stripMargin
    )

    sql04.show()
    //默认输出
    sql04.write.json("data\\uselog\\requst05\\sql1")
  }
}
