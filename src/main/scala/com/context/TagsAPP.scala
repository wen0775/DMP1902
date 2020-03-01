package com.context

import com.util.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsAPP extends Tags{
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 参数解析
    val row = args(0).asInstanceOf[Row]
    val map = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

    // 获取appname,appid
    val appname = row.getAs[String]("appname")
    val appid = row.getAs[String]("appid")
    if(StringUtils.isNotBlank(appname)){
      list:+=("APP"+appname,1)
    }else{
      list:+=("AAP"+map.value.getOrElse(appid,"其他"),1)
    }
    list
  }
}
