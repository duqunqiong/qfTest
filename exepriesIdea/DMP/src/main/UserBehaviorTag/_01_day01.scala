import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @description 项目数据处理
  * @author duqunqiong
  * @date 2020/6/15 21:08
  *
  */

object _01_day01 {
  def main(args: Array[String]): Unit = {

    // 判断输入输出路径
    if(args.length == 0){
      sys.exit()
    }
    val loadPath: String = args(0)
    val writePath: String = args(1)


    // 创建执行入口
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("DrawOfUser")
      .master("local")
      .getOrCreate()

    //加载数据  --- 方法一
    val lineRDD: RDD[String] = sparkSession.sparkContext.textFile("data\\textLog.log")
    lineRDD.map(_.split(","))

//    // 数据处理
//    val list = List(List(1,2,3,4,5,6,7,8,9),List(1,2,3,4,5,6,7,8,9))
//
//    val rdd = sparkSession.sparkContext.parallelize(list)
//    val value: RDD[(Int, List[Int])] = rdd.map((1, _))
//
//    val sum: RDD[(Int, List[Int])] = sum.reduceByKey((list1, list2) => {
//      // 提示用拉链
//      val tuples: List[(Int, Int)] = list1.zipAll(list2)
//      val value1: List[Int] = tuples.map(t => t._2 + t._1)
//      value1
//    })



    // 加载数据 --- 方法二
    val dataDF: DataFrame = sparkSession.read.text("data\\textLog.log")
    val sql1: DataFrame = sparkSession.sql(
      """

      """.stripMargin
    )
    sql1

  }
}
