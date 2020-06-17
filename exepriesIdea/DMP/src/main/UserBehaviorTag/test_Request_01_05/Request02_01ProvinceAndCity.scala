package test_Request_01_05

import log_Class.{UserLog, log}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  *
  * @description
  * 指标	        说明
  * 原始请求数	    发来的所有原始请求数
  * 有效请求数	    筛选满足有效条件的请求数量
  * 广告请求数	    筛选满足广告请求条件的请求数量
  * 参与竞价数	    参与竞价的次数
  * 竞价成功数	    成功竞价的次数
  * 展示数	        针对广告主统计：广告在终端实际被展示的数量
  * 点击数	        针对广告主统计：广告展示后被受众实际点击的数量
  * DSP广告消费	  相对于投放DSP广告的广告主来说满足广告成功展示每次消费WinPrice/1000
  * DSP广告成本	  相对于投放DSP广告的广告主来说满足广告成功展示每次成本adpayment/1000
  * @author duqunqiong
  * @date 2020/6/16 20:29
  *
  */

object Request02_01ProvinceAndCity {
  def main(args: Array[String]): Unit = {
    // 得到参数
    val log: (RDD[log], SparkSession) = new UserLog().creatUserLog
    val logUser: RDD[log] = log._1
    val sparkSession: SparkSession = log._2

//    // 隐式转换
//    import sparkSession.implicits._
//
//    val userLogDF: DataFrame = logUser.toDF()
//    // userLogDF.show()
//
//    userLogDF.createTempView("userLog")
//
//    // val dataDF: DataFrame = sparkSession.read.text("data\\textLog.log")
//    val sql1: DataFrame = sparkSession.sql(
//      """ select
//        |sum(case when requestmode == 1 and processnode >= 1 then 1 else 0 end) orgRequest,
//        |sum(case when requestmode == 1 and processnode >= 2 then 1 else 0 end) infRequest,
//        |sum(case when requestmode == 1 and processnode == 3 then 1 else 0 end) advRequest,
//        |sum(case when isbilling == 1 and isbid == 1 and iseffective == 1 then 1 else 0 end) bidNum,
//        |sum(case when isbilling == 1 and iswin == 1 and iseffective == 1 and adorderid != 0 then 1 else 0 end) winBidNum,
//        |sum(case when requestmode == 2 and iseffective == 1 then 1 else 0 end) showNum,
//        |sum(case when requestmode == 3 and iseffective == 1 then 1 else 0 end) clickNum,
//        |sum(case when isbilling == 1 and iseffective == 1 and iswin == 1 then winprice/1000 else 0 end)  winPrice,
//        |sum(case when isbilling == 1 and iseffective == 1 and iswin == 1 then adppprice/1000 else 0 end) adppprice
//        |from userLog
//        |group by provincename,cityname
//      """.stripMargin
//    )

    // sql1.show()
    // //默认输出
    // sql1.write.parquet("data\\uselog\\requst02")

//    // 结果按json输出到 本地磁盘
//    sql1.write.json("file:///D:\\BigData_qf\\Spark项目\\\\datalog\\request01\\sql1")
//
//    // 输出到数据库
//    val url = "mysql://"
//    val table = "userLog"
//    val userName = "root"
//    val password = "123456"
//
//    val properties = new Properties()
//    properties.setProperty( "username","root")
//    properties.setProperty("password","123456")
//
//    sql1.write.jdbc(url,table,properties)
//
   // 用算子实现，并存储到磁盘

    // 数据处理
    val handleRDD = logUser.map(t => {
      val orgRequest = {
        if (t.requestmode == 1 && t.processnode >= 1) 1 else 0
      }
      val infRequest = {
        if (t.requestmode == 1 && t.processnode >= 2) 1 else 0
      }
      val advRequest = {
        if (t.requestmode == 1 && t.processnode == 3) 1 else 0
      }
      val bidNum = {
        if (t.isbilling == 1 && t.isbid == 1 && t.iseffective == 1) 1 else 0
      }
      val winBidNum = {
        if (t.isbilling == 1 && t.iswin == 1 && t.iseffective == 1 && t.adorderid != 0) 1 else 0
      }
      val showNum = {
        if (t.requestmode == 2 && t.iseffective == 1) 1 else 0
      }
      val clickNum = {
        if (t.requestmode == 3 && t.iseffective == 1) 1 else 0
      }
      val winPrice = {
        if (t.isbilling == 1 && t.iseffective == 1 && t.iswin == 1) t.winprice/1000 else 0
      }
      val adppprice = {
        if (t.isbilling == 1 && t.iseffective == 1 && t.iswin == 1) t.adppprice/1000 else 0
      }

   ((t.provincename, t.cityname) ,List(orgRequest, infRequest, advRequest, bidNum, winBidNum, showNum, clickNum, winPrice, adppprice))

})

    //聚合
   // handleRDD

    // 聚合
    val collect: RDD[((String, String), List[Double])] = handleRDD.reduceByKey((list1: List[Double], list2: List[Double]) => {

      val tuples: List[(Double, Double)] = list1.zip(list2)
      val list: List[Double] = tuples.map(t => t._2 + t._1)
      list
    })

    collect.foreach(println)
    sparkSession.stop()

  }
}
