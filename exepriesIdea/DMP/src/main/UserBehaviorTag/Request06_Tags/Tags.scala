package Request06_Tags

import log_Class.log
import org.apache.spark.rdd.RDD

/**
  * 定义标签接口
  */
trait Tags {
    def creatTag(table:RDD[log]):List[(String,Int)]
}
