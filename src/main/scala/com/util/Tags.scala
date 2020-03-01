package com.util

trait Tags {

  /**
    * 定义一个打标签的接口
    */
  def makeTags(args:Any*):List[(String,Int)]
}
