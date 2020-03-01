package com.util

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

/**
  * 标签工具类
  */
object TagUtils {

  val OneUserId =
    """
      |imei !='' or mac !='' or openudid !='' or androidid !='' or idfa !=''
    """.stripMargin

  /**
    * 获取唯一不为空的用户ID
    * @param row
    */
  def getAnyOneUserId(row: Row) = {
    row match {
      case v if StringUtils.isNotBlank(v.getAs[String]("imei")) => "IM:"+v.getAs[String]("imei")
      case v if StringUtils.isNoneBlank(v.getAs[String]("mac")) => "MC:"+v.getAs[String]("mac")
      case v if StringUtils.isNoneBlank(v.getAs[String]("idfa")) => "ID:"+v.getAs[String]("idfa")
      case v if StringUtils.isNoneBlank(v.getAs[String]("openudid")) => "OID:"+v.getAs[String]("openudid")
      case v if StringUtils.isNoneBlank(v.getAs[String]("androidid")) => "AID:"+v.getAs[String]("androidid")
    }
  }

}
