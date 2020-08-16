package com.atguigu.utils

import java.io.InputStreamReader
import java.util.Properties

/**
 * @Author: Sdaer
 * @Date: 2020-08-16
 * @Desc:
 *       获取配置文件内容，并加载
 */
object PropertiesUtil {

  def load(propertieName: String): Properties = {

    val prop = new Properties()

    prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName), "UTF-8"))

    prop
  }

}
