package com.util


/**
 * 处理指标
 */
object RtbUtils {
	/**
	 * 处理所有请求指标
	 * @param requestmode
	 * @param processnode
	 * @return
	 */
	def requestAd(requestmode:Int, processnode:Int):List[Double] = {
		if (requestmode == 1 && processnode ==1){
			// 第一个元素代表  原始请求
			// 第二个元素代表  有效请求
			// 第三个元素代表  广告请求
			List[Double](1,0,0)
		}else if (requestmode == 1 && processnode == 2){
			List[Double](1,1,0)
		}else if (requestmode == 1 && processnode == 3){
			List[Double](1,1,1)
		}else {
			List[Double](0,0,0)
		}
	}

	/**
	 * 处理参与竞价，广告资金
	 * @param isseffective
	 * @param isbilling
	 * @param isbid
	 * @param iswin
	 * @param adorderid
	 * @param winprince
	 * @param adpayment
	 * @return
	 */
	def adPrice(
	           isseffective:Int, isbilling:Int, isbid:Int, iswin:Int,
	           adorderid:Int, winprince:Double, adpayment:Double): List[Double] = {
		if (isseffective == 1 && isbilling == 1 && isbid == 1){
			if(isseffective == 1 && isbilling == 1 && iswin == 1 && adorderid != 0){
				List[Double](1,1,winprince/1000,adpayment/1000)
			}else {
				List[Double](1,0,0,0)
			}
		}else {
			List[Double](0,0,0,0)
		}
	}

	/**
	 * 展示、点击
	 * @param requestmode
	 * @param iseffective
	 * @return
	 */
	def shows(requestmode:Int, iseffective:Int): List[Double] = {
		if(requestmode ==2 && iseffective ==1){
			List[Double](1,0)
		}else if(requestmode == 3 && iseffective ==1){
			List[Double](0,1)
		}else{
			List[Double](0,0)
		}
	}
}
