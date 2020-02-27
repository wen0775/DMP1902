package com.app

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * 地域维度指标
 */
object LocationRpt {
	def main(args: Array[String]): Unit = {
		// 配置hadoop环境变量
		System.setProperty("hadoop.home.dir","D:\\app\\hadoop\\hadoop-2.7.7")
		// 判断目录是否正确
		if(args.length!=2){
			println("目录不正确，退出")
			sys.exit()
		}
		val Array(inputPath,outputPath)=args
		// 配置序列化方式
		val conf = new SparkConf()
			.setAppName(this.getClass.getName)
			.setMaster("local")
			.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder().config(conf).getOrCreate()
		// 配置SQLContext的压缩方式，默认其实就是snappy 压缩，在2.0以前不是snappy压缩
		spark.sqlContext.setConf("spark.sql.parquet.compression.codec","snappy")
		// 获取数据源
		val df = spark.read.parquet(inputPath)
		// 注册临时视图
		df.createTempView("log")
		val reuslt = spark.sql(
			"""
			  |select provincename,cityname,
			  |sum(case when requestmode =1 and processnode >= 1 then 1 else 0 end) ysrequest,
			  |sum(case when requestmode =1 and processnode >= 2 then 1 else 0 end) yxrequest,
			  |sum(case when requestmode =1 and processnode = 3 then 1 else 0 end) adrequest,
			  |sum(case when iseffective =1 and isbilling =1 and isbid =1 then 1 else 0 end) cybid,
			  |sum(case when iseffective =1 and isbilling =1 and iswin =1 and adorderid !=0 then 1 else 0 end) cyAccbid,
			  |sum(case when requestmode =2 and iseffective =1 then 1 else 0 end) shows,
			  |sum(case when requestmode =3 and iseffective =1 then 1 else 0 end) clicks,
			  |sum(case when iseffective =1 and isbilling =1 and iswin =1 then winprice/1000 else 0.0 end) pricost,
			  |sum(case when iseffective =1 and isbilling =1 and iswin =1 then adpayment/1000 else 0.0 end) adpay
			  |from log group by provincename,cityname
      """.stripMargin)
		// 数据存储
		reuslt.write.partitionBy("provincename","cityname").save(outputPath)

		spark.stop()

	}
}

