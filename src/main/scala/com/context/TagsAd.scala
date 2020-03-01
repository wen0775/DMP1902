package com.context

import com.util.Tags
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAd extends Tags{
  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()
    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取广告类型
    val adType = row.getAs[Int]("adspacetype")
    val adName = row.getAs[String]("adspacetypename")
    adType match {
      case v if v>9 => list:+=("LC"+v,1)
      case v if v>0 && v<=9 =>list:+=("LC0"+v,1)
    }
    if(StringUtils.isNotBlank(adName)){
      list:+=("LN"+adName,1)
    }

    /**
      * 渠道标签
      */
    val adplatformproviderid = row.getAs[Int]("adplatformproviderid")
    if(StringUtils.isNotBlank(adplatformproviderid.toString)){
      list:+=("CN"+adplatformproviderid,1)
    }
    list
  }
}
