import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *
  * @description 项目数据处理
  * @author duqunqiong
  * @date 2020/6/15 21:08
  *
  */

object DrawForUser {
  def main(args: Array[String]): Unit = {

//    // 判断输入输出路径
//    if(args.length != 2){
//      println("文件目录不准确，请重新输入")
//      sys.exit()
//    }
//  //  val Array(loadPath,writePath) = args
//    val loadPath: String = args(0)
//    val writePath: String = args(1)


    // 创建执行入口
    val conf: SparkConf = new SparkConf().set("spark.serializer","org.apache.spark.serializer.KryoSerializer")

    val sparkSession: SparkSession = SparkSession.builder()
      .appName("DrawOfUser")
      .master("local")
      .config(conf)
      .getOrCreate()

    //加载数据
    val lineRDD: RDD[String] = sparkSession.sparkContext.textFile("data\\textLog.log")

    // 过滤数据，在切分的时候，字符相连或者相连过长，使用split切分，会默认把他当成一个字符处理，
    // 那这样会导致数据不准确，同时过滤的数据太多，在切分的时候，最好加上字符串长度 -1 即可
    val logUser: RDD[log] = lineRDD.filter(_.split(",").length >= 85).map(t => {
      val arr = t.split(",", -1)

      // 如果我们使用类，而不是样例类，最多只能使用22个字段变量
      // 那么想要使用类超过22个字段，需要继承product的特质
      // 也可以使用样例类 case class
      new log(
        arr(0),
        TypeUtils.str2Int(arr(1)),
        TypeUtils.str2Int(arr(2)),
        TypeUtils.str2Int(arr(3)),
        TypeUtils.str2Int(arr(4)),
        arr(5),
        arr(6),
        TypeUtils.str2Int(arr(7)),
        TypeUtils.str2Int(arr(8)),
        TypeUtils.str2Double(arr(9)),
        TypeUtils.str2Double(arr(10)),
        arr(11),
        arr(12),
        arr(13),
        arr(14),
        arr(15),
        arr(16),
        TypeUtils.str2Int(arr(17)),
        arr(18),
        arr(19),
        TypeUtils.str2Int(arr(20)),
        TypeUtils.str2Int(arr(21)),
        arr(22),
        arr(23),
        arr(24),
        arr(25),
        TypeUtils.str2Int(arr(26)),
        arr(27),
        TypeUtils.str2Int(arr(28)),
        arr(29),
        TypeUtils.str2Int(arr(30)),
        TypeUtils.str2Int(arr(31)),
        TypeUtils.str2Int(arr(32)),
        arr(33),
        TypeUtils.str2Int(arr(34)),
        TypeUtils.str2Int(arr(35)),
        TypeUtils.str2Int(arr(36)),
        arr(37),
        TypeUtils.str2Int(arr(38)),
        TypeUtils.str2Int(arr(39)),
        TypeUtils.str2Double(arr(40)),
        TypeUtils.str2Double(arr(41)),
        TypeUtils.str2Int(arr(42)),
        arr(43),
        TypeUtils.str2Double(arr(44)),
        TypeUtils.str2Double(arr(45)),
        arr(46),
        arr(47),
        arr(48),
        arr(49),
        arr(50),
        arr(51),
        arr(52),
        arr(53),
        arr(54),
        arr(55),
        arr(56),
        TypeUtils.str2Int(arr(57)),
        TypeUtils.str2Double(arr(58)),
        TypeUtils.str2Int(arr(59)),
        TypeUtils.str2Int(arr(60)),
        arr(61),
        arr(62),
        arr(63),
        arr(64),
        arr(65),
        arr(66),
        arr(67),
        arr(68),
        arr(69),
        arr(70),
        arr(71),
        arr(72),
        TypeUtils.str2Int(arr(73)),
        TypeUtils.str2Double(arr(74)),
        TypeUtils.str2Double(arr(75)),
        TypeUtils.str2Double(arr(76)),
        TypeUtils.str2Double(arr(77)),
        TypeUtils.str2Double(arr(78)),
        arr(79),
        arr(80),
        arr(81),
        arr(82),
        arr(83),
        TypeUtils.str2Int(arr(84))
      )
    })



    // 处理数据 --- 方法一


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


    /**
      * 需求二 ：地域消费状况
      */
    // 隐式转换
    import sparkSession.implicits._

    val userLog: RDD[log] = logUser.coalesce(1)
    val userLogDF: DataFrame = userLog.toDF()
// userLogDF.show()

    userLogDF.createTempView("userLog")

   // val dataDF: DataFrame = sparkSession.read.text("data\\textLog.log")
    val sql1: DataFrame = sparkSession.sql(
      """ select
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
         |group by provincename,cityname
      """.stripMargin
    )

    sql1.show()
    //默认输出
     sql1.write.parquet("data\\uselog")

    sparkSession.stop()

  }
}
