package com.app

import com.util.RtbUtils
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
 * 地域维度指标 core实现
 */
object LocationRptV2 {
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
		// 获取字段数据
		val word: RDD[((String, String), List[Double])] = df.rdd.map(row => {
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
			// 获取省市
			val pro = row.getAs[String]("provincename")
			val city = row.getAs[String]("cityname")
			((pro, city), reqList ++ adList ++ clickList)
		})
		word.reduceByKey((list1,list2)=>{
			// list1 (1,1,0,0,0)  list2(1,1,0,0,0)
			// zip((1,1),(1,1),(0,0),(0,0),(0,0))
			list1.zip(list2)
				// List(2,2,0,0,0)
				.map(t=>t._1+t._2)
		}).foreach(println)

	}
}

