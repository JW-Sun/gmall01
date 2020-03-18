package com.real.apps

import java.util

import com.alibaba.fastjson.JSON
import com.real.GmallConstants
import com.real.bean.{OrderDetail, OrderInfo, SaleDetail}
import com.real.utils.{MyKafkaUtil, RedisUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.mutable.ListBuffer

object SaleApp {
	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setAppName("SaleApp").setMaster("local[*]")

		val ssc = new StreamingContext(sparkConf, Seconds(5))

		val orderInfoInputDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_NEW_ORDER, ssc)

		val orderDetailInputDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_ORDER_DETAIL, ssc)

		val userInfoDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_USER_INFO, ssc)

		/*1.消费两个kafka的数据OrderInfo和OrderDetail并且这两个流转化成为caseclass
		* 	然后进行双流join，关联订单的信息，然后在进行redis的反查
		*
		* 2.redis 的反查主要就是消费kafka的数据准化成caseclass JSON。parseObject
		* 	保存到redis中
		*
		* 3. 最后保存到elasticsearch*/

		//1. 将流 变成样例类 创建case class OrderDetail UserInfo SaleDetail
		val orderInfoDStream: DStream[OrderInfo] = orderInfoInputDStream.map { item: ConsumerRecord[String, String] =>
			val orderInfo: OrderInfo = JSON.parseObject(item.value(), classOf[OrderInfo])

			//补充时间字段
			val dateTimeArr: Array[String] = orderInfo.create_time.split(" ")
			orderInfo.create_date = dateTimeArr(0)
			val hourStr: String = dateTimeArr(1).split(":")(0)
			orderInfo.create_hour = hourStr

			//脱敏
			val tuple: (String, String) = orderInfo.consignee_tel.splitAt(4)
			orderInfo.consignee_tel = tuple._1 + "********"

			orderInfo
		}

		//转换成（k，v）结构
		val orderId2InfoDStream: DStream[(String, OrderInfo)] =
			orderInfoDStream.map((orderInfo: OrderInfo) => (orderInfo.id, orderInfo))

//		orderId2InfoDStream.foreachRDD {rdd: RDD[(String, OrderInfo)] =>
//			println(rdd.collect().mkString("\n"))
//		}

		//得到OrderDetail的流
		val orderDetailDStream: DStream[OrderDetail] = orderDetailInputDStream.map { item: ConsumerRecord[String, String] =>
			val jsonStr: String = item.value()
			val orderDetail: OrderDetail = JSON.parseObject(jsonStr, classOf[OrderDetail])

			orderDetail
		}

		//得到（ordreId， orderDetail）的流
		val orderId2DetailDStream: DStream[(String, OrderDetail)] =
			orderDetailDStream.map((item: OrderDetail) => (item.order_id, item))

//		orderId2DetailDStream.foreachRDD {rdd =>
//			println(rdd.collect().mkString("\n"))
//		}


		/*orderInfo 和 orderDetail 通过id进行join
		双流join的时候需要考虑在不同批次的时候，两边都不能进行放弃 使用fullJoin*/
		val fullJoinDStream: DStream[(String, (Option[OrderInfo], Option[OrderDetail]))] =
			orderId2InfoDStream.fullOuterJoin(orderId2DetailDStream)

		//流中的元素都是每一个主表和从表的结合体
		val saleDetailDStream: DStream[SaleDetail] =
			fullJoinDStream.flatMap { case (orderId, (infoOpt, detailOpt)) =>

				/*
				1. info和detail都有，那么关联成功，组合成SaleDetail
						detail无  -》 缓存中查询是否有detail
				2. 关联或没关联上都写入到缓存中
				3. info无 detail有 查询缓存中是否存有info
				* */

				//如果orderInfo不为空
				//如果从表detail也不为None 那么关联从表
				//把orderinfo自己写入缓存
				//查询缓存
				//如果主表info为空， 从表detail不为空
				//把detail自己写入缓存
				//查询缓存

				/*将case class转换成json串的scala的特有工具*/
				import org.json4s.native.Serialization
				implicit val formats = org.json4s.DefaultFormats

				var saleDetailList: ListBuffer[SaleDetail] = new ListBuffer[SaleDetail]()
				val jedisClient: Jedis = RedisUtil.getJedisClient

				//主表info不为空
				if (infoOpt.isDefined) {
					val orderInfo: OrderInfo = infoOpt.get
					//1. 附表不为空
					if (detailOpt.isDefined) {
						val orderDetail: OrderDetail = detailOpt.get
						//合并为宽表对象
						val saleDetail = new SaleDetail(orderInfo, orderDetail)
						saleDetailList += saleDetail
					}

					//2. 主表自己存入redis缓存 考虑使用的类型string


					val orderInfoKey: String = "order_info=" + orderInfo.id

					//fastJson在解析case class时会出现错误
					//使用专业scala工具处理case class json4s


					val orderInfoJson: String = Serialization.write(orderInfo)

					jedisClient.setex(orderInfoKey, 3600, orderInfoJson)


					//3. 查询缓存中有没有与主表相符的从表，从而进行连接
					// 订单明细如何保存到redis中 使用不重复的set集合  key:order_detail:order_id
					val orderDetailKey = "order_detail=" + orderInfo.id
					//进入redis查询
					val orderDetailSet: util.Set[String] = jedisClient.smembers(orderDetailKey)

					import scala.collection.JavaConversions._
					for (orderDetailJson <- orderDetailSet) {
						val orderDetail: OrderDetail =
							JSON.parseObject(orderDetailJson, classOf[OrderDetail])
						val saleDetail = new SaleDetail(orderInfo, orderDetail)
						saleDetailList += saleDetail
					}
				} else if (detailOpt.isDefined) {

					//如果主表info为空， 从表detail不为空
					//把自己写入缓存
					//查询缓存

					//存入redis set key order_detail:order_id  value:-> order_detail_json
					val orderDetail: OrderDetail = detailOpt.get
					val orderDetailJson: String = Serialization.write(orderDetail)
					val orderDetailKey = "order_detail=" + orderDetail.order_id

					jedisClient.sadd(orderDetailKey, orderDetailJson)
					jedisClient.expire(orderDetailKey, 3600)

					//查询缓存中的info主表 缓存类型是string型
					val orderInfoKey = "order_info=" + orderDetail.order_id
					val orderInfoJson: String = jedisClient.get(orderInfoKey)
					if (orderInfoJson != null && orderInfoJson.length > 0) {
						val orderInfo: OrderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
						val sale = new SaleDetail(orderInfo, orderDetail)
						saleDetailList += sale
					}
				}
				jedisClient.close()

				saleDetailList
			}

		saleDetailDStream.foreachRDD {rdd: RDD[SaleDetail] =>
			println(rdd.collect().mkString("\n"))
		}


		println("start")
		ssc.start()
		ssc.awaitTermination()
	}
}
