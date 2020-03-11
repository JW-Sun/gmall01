package com.real.utils

import java.io.InputStreamReader
import java.util.Properties

object PropertiesUtil {
	def main(args: Array[String]): Unit = {
		val properties: Properties = PropertiesUtil.load("config.properties")

		println(properties.getProperty("redis.host"))
	}

	def load(propertiesName: String): Properties = {
		val prop = new Properties()
		prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertiesName)))

		prop
	}
}
