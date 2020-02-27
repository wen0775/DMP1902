package com.etl

import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

/**
 * 指标统计
 */
object Data2MySQL1 {

	def main(args: Array[String]): Unit = {
		// 配置hadoop环境变量
		System.setProperty("hadoop.home.dir","D:\\app\\hadoop\\hadoop-2.7.7")
		if(args.length != 2){
			println("目录不正确，退出！")
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
		// 获取数据
		val df = spark.read.parquet(inputPath)
		// 注册临时视图
		df.createTempView("log")
		// 执行SQL
		val result = spark.sql("select count(*) ct,provincename,cityname from log group by provincename,cityname")
		    result.coalesce(1).write.json(outputPath)
		// 加载配置文件 默认加载resources中的配置文件（.conf  .json   .properties）
//		val load = ConfigFactory.load()
//		val prop = new Properties()
//		prop.setProperty("user",load.getString("jdbc.user"))
//		prop.setProperty("password",load.getString("jdbc.password"))
//		result.write.mode(SaveMode.Append).jdbc(
//			load.getString("jdbc.url"),load.getString("jdbc.tableName"),prop)
	}
}

