package com.real.utils

import java.util
import java.util.Objects

import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}

object MyESUtil {

	private val ES_HOST = "http://hadoop02"
	private val ES_HTTP_PORT = 9200
	private var factory:JestClientFactory = null

	/**
	 * 获取客户端
	 *
	 * @return jestclient
	 */
	def getClient: JestClient = {
		if (factory == null) build()
		factory.getObject
	}

	/**
	 * 关闭客户端
	 */
	def close(client: JestClient): Unit = {
		if (!Objects.isNull(client)) try
			client.shutdownClient()
		catch {
			case e: Exception =>
				e.printStackTrace()
		}
	}

	/**
	 * 建立连接
	 */
	private def build(): Unit = {
		factory = new JestClientFactory
		factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
		  .maxTotalConnection(20) //连接总数
		  .connTimeout(10000).readTimeout(10000).build)

	}

	/***
	 * 批量插入ES
	 */
	def indexBulk(indexName: String, dataList: List[(String, Any)]): Unit = {
		val jestClient: JestClient = getClient
		val bulkBuilder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")

		for ((id, doc) <- dataList) {
			val indexBuilder = new Index.Builder(doc)
			if (id != null) {
				indexBuilder.id(id)
			}
			val index: Index = indexBuilder.build()
			bulkBuilder.addAction(index)
		}

		val bulk: Bulk = bulkBuilder.build()

		val items: util.List[BulkResult#BulkResultItem] = jestClient.execute(bulk).getItems
		println(s"保存了${items.size()}条")

		close(jestClient)
	}
}
