package com.context

import ch.hsr.geohash.GeoHash
import com.util.{AmapUtil, JedisConnectionPool, Tags}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsBusiness extends Tags{


  /**
    * 定义一个打标签的接口
    */
  override def makeTags(args: Any*): List[(String, Int)] = {
    var list = List[(String,Int)]()

    // 解析参数
    val row = args(0).asInstanceOf[Row]
    // 获取经纬度，那么要考虑经纬度是否正确
    if(row.getAs[String]("long").toDouble>=73
      && row.getAs[String]("long").toDouble<=135
      && row.getAs[String]("lat").toDouble >= 3
      && row.getAs[String]("lat").toDouble <= 53){
      // 获取经纬度
      val long = row.getAs[String]("long")
      val lat = row.getAs[String]("lat")
      // 通过经纬度获取商圈
      val business = getBusiness(long.toDouble,lat.toDouble)
      if(StringUtils.isNotBlank(business)){
        // 对商圈切分
        val str = business.split(",")
        str.foreach(t=>{
          list :+=(t,1)
        })
      }
    }
    // 一条数据进行内部商圈去重
    list.distinct
  }


  /**
    * 获取商圈
    *
    * @param long
    * @param lat
    */
  def getBusiness(long: Double, lat: Double) = {
    // 将经纬度转换成GeoHash编码
    val geoHash = GeoHash.geoHashStringWithCharacterPrecision(lat,long,8)
    // 去数据库查询
    var business = redis_queryBusiness(geoHash)
    // 判断是否获取到商圈
    if(business == null || business.length ==0){
      // 通过调用第三方接口 获取商圈
      business = AmapUtil.getBusinessFromAMap(long,lat)
      // 将商圈保存到数据库
      if(business !=null &&  business.length>0){
        redis_insertBusiness(geoHash,business)
      }
    }
    business
  }

  /**
    * 查询商圈
    * @param geoHash
    */
  def redis_queryBusiness(geoHash: String) = {
    val jedis = JedisConnectionPool.getConnection()
    val str = jedis.get(geoHash)
    jedis.close()
    str
  }

  /**
    * 将数据保存
    * @param geoHash
    * @param business
    * @return
    */
  def redis_insertBusiness(geoHash: String, business: String) = {
    val jedis = JedisConnectionPool.getConnection()
    jedis.set(geoHash,business)
    jedis.close()
  }
}
