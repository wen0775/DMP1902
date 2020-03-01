package com.context

import com.util.Tags
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord extends Tags{
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    val map = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]
    // 取值判断
    val kw = row.getAs[String]("keywords").split("\\|")
    // 两个条件  第一个满足长度，第二个满足停用词库
    kw.filter(
      word=>word.length>=3 && word.length<=8 && !map.value.contains(word))
      .foreach(word=>list:+=("K"+word,1))
    list
  }
}
