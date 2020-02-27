package com.app

import com.util.RtbUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.StringUtils


/**
 *  媒体指标
 */
object appRpt {
	def main(args: Array[String]): Unit = {
		// 配置hadoop环境变量
		System.setProperty("hadoop.home.dir","D:\\app\\hadoop\\hadoop-2.7.7")
		// 获取参数数值
		if (args.length != 3) {
			println("参数不正确，退出程序！")
			sys.exit()
		}
		val Array(inputPath, outputPath, dic) = args
		// 配置序列化方式
		val conf = new SparkConf()
    		.setAppName("this.getClass.getName")
    		.setMaster("local[*]")
    		.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder().config(conf).getOrCreate()
		// 配置SQLContext的压缩方式，默认其实就是snappy 压缩，在2.0以前不是snappy压缩
		spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
		// 读取字典文件
		val dicMap = spark.sparkContext.textFile(dic)
			.map(_.split("\t",-1)).filter(_.length>=5)
			.map(arr=>{
				(arr(4),arr(1))
			}).collectAsMap()
		// 广播
		val broadCastDic = spark.sparkContext.broadcast(dicMap)
		// 获取数据源
		val df = spark.read.parquet(inputPath)
		df.rdd.map(row=>{
			var appname = row.getAs[String]("appname")
			// 判断是否为空
			if(!StringUtils.isNoneBlank(appname)){
				appname = broadCastDic.value.getOrElse(row.getAs[String]("appid"),"其他")
			}
			// 获取需要的字段
			val requestmode = row.getAs[Int]("requestmode")
			val processnode = row.getAs[Int]("processnode")
			val iseffective = row.getAs[Int]("iseffective")
			val isbilling = row.getAs[Int]("isbilling")
			val isbid = row.getAs[Int]("isbid")
			val iswin = row.getAs[Int]("iswin")
			val adorderid = row.getAs[Int]("adorderid")
			val winprice = row.getAs[Double]("winprice")
			val adpayment = row.getAs[Double]("adpayment")
			// 调用业务方法
			val reqList = RtbUtils.requestAd(requestmode, processnode)
			val adList = RtbUtils.adPrice(iseffective, isbilling,
				isbid, iswin, adorderid, winprice, adpayment)
			val clickList = RtbUtils.shows(requestmode, iseffective)
			(appname,reqList++adList++clickList)
		}).reduceByKey((list1,list2)=>{
			list1.zip(list2).map(t=>t._1+t._2)
		}).foreach(println)
	}
}
