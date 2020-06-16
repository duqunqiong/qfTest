import java.util.Properties

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @description
  * * 要求一：
  * * 将统计的结果输出成 json 格式，并输出到磁盘目录。
  * * {"ct":943,"provincename":"内蒙古自治区","cityname":"阿拉善盟"}
  * * {"ct":578,"provincename":"内蒙古自治区","cityname":"未知"}
  * * {"ct":262632,"provincename":"北京市","cityname":"北京市"}
  * * {"ct":1583,"provincename":"台湾省","cityname":"未知"}
  * * {"ct":53786,"provincename":"吉林省","cityname":"长春市"}
  * * {"ct":41311,"provincename":"吉林省","cityname":"吉林市"}
  * * {"ct":15158,"provincename":"吉林省","cityname":"延边朝鲜族自治州"}
  * *
  * * 要求二：
  * * 将结果写到到 mysql 数据库。
  * *
  * * 要求三：
  * * 用 spark 算子的方式实现上述的统计，存储到磁盘。
  * @author duqunqiong
  * @date 2020/6/16 20:28
  *
  */

object Request01 {
  def main(args: Array[String]): Unit = {

    // 得到参数
    val log: (RDD[log], SparkSession) = new UserLog().creatUserLog
    val logUser: RDD[log] = log._1
    val sparkSession: SparkSession = log._2

    import sparkSession.implicits._

    val DF: DataFrame = logUser.toDF()

    DF.createTempView("userLog")

    val sql1: DataFrame = sparkSession.sql(
      """
          select
          ct,
          provincename,
          cityname
          from userLog
      """.stripMargin)

    // 存到磁盘
    sql1.write.json("file:///D:\\BigData_qf\\Spark项目\\datalog\\request1")

    //存到数据库
    val url = "mysql://"
    val table = "userLog"
    val userName = "root"
    val password = "123456"

    val properties = new Properties()
    properties.setProperty( "username","root")
    properties.setProperty("password","123456")

    sql1.write.jdbc(url,table,properties)
    // 算子实现，存到磁盘


    sparkSession.stop()
  }
}
