package com.real.apps

import java.util

import com.alibaba.fastjson.JSON
import com.real.GmallConstants
import com.real.bean.{CouponAlertInfo, EventInfo}
import com.real.utils.{MyESUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object AlertApp {
	def main(args: Array[String]): Unit = {

		val sparkConf: SparkConf = new SparkConf().setAppName("AlertApp").setMaster("local[*]")

		val streamingContext = new StreamingContext(sparkConf, Seconds(5))

		val inputDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_EVENT, streamingContext)

		//1. 格式转换成为样例类
		val eventInfoDStream: DStream[EventInfo] = inputDStream.map { item: ConsumerRecord[String, String] =>
			val info: EventInfo = JSON.parseObject(item.value(), classOf[EventInfo])
			info
		}

		/*2. 5分钟的窗口 滑动窗口*/
		val eventWindowDStream: DStream[EventInfo] =
			eventInfoDStream.window(Seconds(300), Seconds(5))

		/*3. 同一个设备 groupby mid*/
		val groupByMidDStream: DStream[(String, Iterable[EventInfo])] =
			eventWindowDStream.map(item => (item.mid, item)).groupByKey()

		/*4. 使用不同的账号登录并且领取优惠券，没有浏览过商品
		* 	 把满足预警条件的mid打上标签*/
		val checkedAlertDStream: DStream[(Boolean, CouponAlertInfo)] =
			groupByMidDStream.map { case (mid, eventInfoIter) =>

			//判断符合预警的条件：
			// 	1. 点击购物券时设计登录账号 大于等于 3
			//  3. event does not contain clickItems

			//账号的HashSet
			val couponUidSet = new util.HashSet[String]()
			val itemSet = new util.HashSet[String]()
			val eventIds = new util.ArrayList[String]()


			//标记是否点击过商品
			import scala.util.control.Breaks._
			var isClickItem = false
			breakable(

				for (eventInfo: EventInfo <- eventInfoIter) {
					eventIds.add(eventInfo.evid) //用户行为
					if (eventInfo.evid == "coupon") {
						couponUidSet.add(eventInfo.uid)
						itemSet.add(eventInfo.itemid)
					}
					if (eventInfo.evid == "clickItem") {
						isClickItem = true
						break()
					}
				}

			)
			//返回的结构是（是否符合预警条件，预警日志信息）
			(couponUidSet.size() >= 3 && !isClickItem, CouponAlertInfo(mid, couponUidSet, itemSet, eventIds, System.currentTimeMillis()))
		}

		//得到满足预警条件的第二个数据部分
		val filteredDStream: DStream[CouponAlertInfo] =
			checkedAlertDStream.filter(_._1 == true).map(_._2)

		/*要将预警的数据保存到elasticsearch中*/
		filteredDStream.foreachRDD {rdd: RDD[CouponAlertInfo] =>
			rdd.foreachPartition {alertIterator: Iterator[CouponAlertInfo] =>
				val alertList: List[CouponAlertInfo] = alertIterator.toList

				//提取主键 mid+分钟 同时利用主键进行去重
				val midMinute2AlertInfo: List[(String, CouponAlertInfo)] =
					alertList.map(alertInfo => (alertInfo.mid +"_"+ alertInfo.ts/1000/60, alertInfo))

				MyESUtil.indexBulk(GmallConstants.ES_INDEX_ALERT, midMinute2AlertInfo)
			}
		}



		println("start")
		streamingContext.start()
		streamingContext.awaitTermination()
	}
}
