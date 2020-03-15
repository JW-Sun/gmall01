package com.real.apps

import com.alibaba.fastjson.JSON
import com.real.GmallConstants
import com.real.bean.OrderInfo
import com.real.utils.MyKafkaUtil
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object OrderApp {
	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setAppName("OrderApp").setMaster("local[*]")

		val streamingContext = new StreamingContext(sparkConf, Seconds(5))

		val inputDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, streamingContext)

		//补充时间戳
		//敏感字段，脱敏，电话，收件人，地址
		val orderDStream: DStream[OrderInfo] = inputDStream.map { item: ConsumerRecord[String, String] =>
			val jsonStr: String = item.value()
			val order: OrderInfo = JSON.parseObject(jsonStr, classOf[OrderInfo])

			val dateStr: Array[String] = order.create_time.split(" ")

			order.create_date = dateStr(0)
			order.create_hour = dateStr(1).split(":")(0)

			//前四位后四位分开了
			val tuple: (String, String) = order.consignee_tel.splitAt(4)
			order.consignee_tel = tuple._1 + "*********"

			order
		}

		//保存HBase + Phoenix
		import org.apache.phoenix.spark._

		orderDStream.foreachRDD { rdd: RDD[OrderInfo] =>
			rdd.saveToPhoenix("GMALL01_ORDER_INFO",
				Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
				new Configuration,
				Some("192.168.159.102,192.168.159.103,192.168.159.104:2181")
			)
		}

		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
