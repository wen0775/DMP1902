package com.context

import com.util.TagUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 上下文标签，用于合并所有标签
  */
object TagsContext {

  def main(args: Array[String]): Unit = {

    // 配置hadoop环境变量
    System.setProperty("hadoop.home.dir","D:\\Huohu\\下载\\hadoop-common-2.2.0-bin-master")
    // 判断目录是否正确
    if(args.length!=2){
      println("目录不正确，退出")
      sys.exit()
    }
    val Array(inputPath,outputPath,dic,stopPath)=args
    // 配置序列化方式
    val conf = new SparkConf()
      .setAppName(this.getClass.getName)
      .setMaster("local")
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val spark = SparkSession.builder().config(conf).getOrCreate()
    // 配置SQLContext的压缩方式，默认其实就是snappy 压缩，在2.0以前不是snappy压缩
    spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")

    // 处理字典文件
    val words = spark.sparkContext.textFile(dic)
      .map(_.split("\t",-1))
      .filter(_.length>=5)
      .map(arr=>{
      (arr(4),arr(1))
    }).collectAsMap()
    // 广播
    val broadCastMap = spark.sparkContext.broadcast(words)
    // 读取停用词库
    val KwMap = spark.sparkContext.textFile(stopPath).map((_,0)).collectAsMap()
    // 广播
    val broadCastKw = spark.sparkContext.broadcast(KwMap)
    // 获取数据源
    val df = spark.read.parquet(inputPath)
    // 处理数据，打标签
    df.filter(TagUtils.OneUserId) // 保证有一个不为空的ID
      .rdd.map(row=>{
      // 首先  先获取UserID
      val userId = TagUtils.getAnyOneUserId(row)
      // 广告标签
      val adTag = TagsAd.makeTags(row)
      // APP标签
      val appTag = TagsAPP.makeTags(row,broadCastMap)
      // 设备标签
      val devTag = TagsDev.makeTags(row)
      // 关键字标签
      val kwTag = TagsKeyWord.makeTags(row,broadCastKw)
      // 获取商圈
      val businessTag = TagsBusiness.makeTags(row)
    })

  }
}
