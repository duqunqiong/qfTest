package Request06_Tags
import log_Class.log
import org.apache.spark.rdd.RDD

/**
  *
  * @description 关键词标签
  * @author duqunqiong
  * @date 2020/6/17 20:07
  *
  */

class Tags_KeyWords extends Tags {
  override def creatTag(table: RDD[log]): List[(String, Int)] = {
    var list: List[(String, Int)] = List[(String,Int)]()

    table.foreach(t=>{
      val words: Array[String] = t.keywords.split("\\|")
      if(words.length >= 3 && words.length <= 8)
        words.foreach(word => list:+=("K"+word,1))
    })

    list
  }
}
