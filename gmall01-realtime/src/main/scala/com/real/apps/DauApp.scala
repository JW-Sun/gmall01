package com.real.apps

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.real.GmallConstants
import com.real.bean.StartUpLog
import com.real.utils.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import redis.clients.jedis.Jedis

object DauApp {


	def main(args: Array[String]): Unit = {
		val sparkConf: SparkConf = new SparkConf().setAppName("DauApp").setMaster("local[*]")

		val sc = new SparkContext(sparkConf)

		val streamingContext = new StreamingContext(sc, Seconds(3))

		val inputDStream: InputDStream[ConsumerRecord[String, String]] =
			MyKafkaUtil.getKafkaDStream(GmallConstants.KAFKA_TOPIC_STARTUP, streamingContext)

		val startUpLogDStream: DStream[StartUpLog] = inputDStream.map {
			item: ConsumerRecord[String, String] => {
				val jsonStr: String = item.value()
				val log: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

				val date = new Date(log.ts)
				val dateStr: String = new SimpleDateFormat("yyyy-MM-dd HH:mm").format(date)

				val split: Array[String] = dateStr.split(" ")

				log.logDate = split(0)
				log.logHour = split(1).split(":")(0)

				log
			}
		}

//		startUpLogDStream.cache()

		/*利用redis进行去重，利用transform*/
		val filteredLogDStream: DStream[StartUpLog] = startUpLogDStream.transform {

			rdd: RDD[StartUpLog] => {
				//这一块在driver中执行,周期之行任务
				println("before filter: " + rdd.count())

				val curDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date())
				val jedis: Jedis = RedisUtil.getJedisClient

				val key = "dau:" + curDate
				val set: util.Set[String] = jedis.smembers(key)

				val dauBC: Broadcast[util.Set[String]] =
					streamingContext.sparkContext.broadcast(set)

				val filterLogRDD: RDD[StartUpLog] = rdd.filter {
					//这里面的就在executor中执行任务
					item: StartUpLog => {
						val set1: util.Set[String] = dauBC.value
						!set1.contains(item.mid)
					}
				}

				println("after filter: " + filterLogRDD.count())

				filterLogRDD
			}
		}

		//刚开始一下冲进来 很多相同mid的话，redis中没有用于去重的元素，就会导致最开始重复的数据冲进来
		// 3秒间隔内的去重， groupby聚合操作
		//继续去重，把相同的mid的数据分成一组，每组取第一个
		val groupByMidDStream: DStream[(String, Iterable[StartUpLog])] =
			filteredLogDStream.map(item => (item.mid, item)).groupByKey()

		val filteredDStream: DStream[StartUpLog] = groupByMidDStream.flatMap {
			case (mid, logIterable) => {
				logIterable.take(1)
			}
		}

		filteredDStream.cache()



		/*  1. 把今日新增的活跃用户保存到redis中
			2. 每条数据经过过滤，去掉redis中已有的用户
		*
		* 	key				value
			dau:2020-03-10	设备id
		* */
		filteredDStream.foreachRDD {
			rdd: RDD[StartUpLog] => {
				//这一部分是在driver中执行
				//--------------------

				rdd.foreachPartition {
					iterator: Iterator[StartUpLog] => {
						//在executor中之行任务
						val jedis: Jedis = RedisUtil.getJedisClient
						for (log <- iterator) {
							val dauKey = "dau:" + log.logDate
							println(dauKey + " " + log.mid)
							jedis.sadd(dauKey, log.mid)
						}

						jedis.close()
					}
				}
			}
		}

		/*3. 导入数据到phoenix中*/
		import org.apache.phoenix.spark._

		filteredDStream.foreachRDD {
			rdd: RDD[StartUpLog] => {
				rdd.saveToPhoenix("GMALL01_DAU", Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
					new Configuration, Some("192.168.159.102,192.168.159.103,192.168.159.104:2181"))
			}
		}


		println("开始计算")
		streamingContext.start()
		streamingContext.awaitTermination()
	}

}
