package com.etl

import com.util.{SchemaUtils, Str2Type}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
//import org.apache.spark.Streaming._


/**
 * 格式转换
 */
object log2Parquet {
	def main(args: Array[String]): Unit = {
		// 配置hadoop环境变量
		System.setProperty("hadoop.home.dir","D:\\app\\hadoop\\hadoop-2.7.7")

		// 获取参数数值
		if (args.length != 2) {
			println("参数不正确，退出程序！")
			sys.exit()
		}
		// 接收参数
		val Array(inputPath, outputPath) = args
		// 配置序列化
		val conf = new SparkConf()
			.setAppName("log2Parquet")
    			.setMaster("local[*]")
    			.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
		val spark = SparkSession.builder().config(conf).getOrCreate()
		// 配置SQLContext的压缩方式，默认其实就是snappy压缩，在2.0前不是snappy
		spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
		// 获取数据源
		val lines: RDD[String] = spark.sparkContext.textFile(inputPath)
		// 切分数据，按照，切分，如果内部的切分关键字相连，那么需要对相连的关键字进行处理，要每一个都切
		val rdd = lines.map(t=>t.split(",",t.length)).filter(_.length>=85).map(arr=>{
			Row(
				arr(0),
				Str2Type.toInt(arr(1)),
				Str2Type.toInt(arr(2)),
				Str2Type.toInt(arr(3)),
				Str2Type.toInt(arr(4)),
				arr(5),
				arr(6),
				Str2Type.toInt(arr(7)),
				Str2Type.toInt(arr(8)),
				Str2Type.toDouble(arr(9)),
				Str2Type.toDouble(arr(10)),
				arr(11),
				arr(12),
				arr(13),
				arr(14),
				arr(15),
				arr(16),
				Str2Type.toInt(arr(17)),
				arr(18),
				arr(19),
				Str2Type.toInt(arr(20)),
				Str2Type.toInt(arr(21)),
				arr(22),
				arr(23),
				arr(24),
				arr(25),
				Str2Type.toInt(arr(26)),
				arr(27),
				Str2Type.toInt(arr(28)),
				arr(29),
				Str2Type.toInt(arr(30)),
				Str2Type.toInt(arr(31)),
				Str2Type.toInt(arr(32)),
				arr(33),
				Str2Type.toInt(arr(34)),
				Str2Type.toInt(arr(35)),
				Str2Type.toInt(arr(36)),
				arr(37),
				Str2Type.toInt(arr(38)),
				Str2Type.toInt(arr(39)),
				Str2Type.toDouble(arr(40)),
				Str2Type.toDouble(arr(41)),
				Str2Type.toInt(arr(42)),
				arr(43),
				Str2Type.toDouble(arr(44)),
				Str2Type.toDouble(arr(45)),
				arr(46),
				arr(47),
				arr(48),
				arr(49),
				arr(50),
				arr(51),
				arr(52),
				arr(53),
				arr(54),
				arr(55),
				arr(56),
				Str2Type.toInt(arr(57)),
				Str2Type.toDouble(arr(58)),
				Str2Type.toInt(arr(59)),
				Str2Type.toInt(arr(60)),
				arr(61),
				arr(62),
				arr(63),
				arr(64),
				arr(65),
				arr(66),
				arr(67),
				arr(68),
				arr(69),
				arr(70),
				arr(71),
				arr(72),
				Str2Type.toInt(arr(73)),
				Str2Type.toDouble(arr(74)),
				Str2Type.toDouble(arr(75)),
				Str2Type.toDouble(arr(76)),
				Str2Type.toDouble(arr(77)),
				Str2Type.toDouble(arr(78)),
				arr(79),
				arr(80),
				arr(81),
				arr(82),
				arr(83),
				Str2Type.toInt(arr(84))
			)
		})
		// 构建DF
		val df = spark.sqlContext.createDataFrame(rdd, SchemaUtils.logStructType)
		// 存储数据
		df.write.parquet(outputPath)
	}
}
