package com.real.service.impl;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.real.GmallConstants;
import com.real.controller.LoggerController;
import com.real.service.LoggerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
public class LoggerServiceImpl implements LoggerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private static final Logger logger = LoggerFactory.getLogger(LoggerController.class);

    /***
     * 获得从外部（可能是虚拟数据）传输进来的数据，然后往Kafka推送
     * @param logJson
     */
    @Override
    public void createLog(String logJson) {
        //1. 补充时间戳
        JSONObject jsonObject = JSON.parseObject(logJson);
        jsonObject.put("ts", System.currentTimeMillis());

        //2. 将日志落盘

        logger.info(jsonObject.toString());


        //3. 发送kafka
        if ("startup".equals(jsonObject.getString("type"))) {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_STARTUP, jsonObject.toString());
        } else {
            kafkaTemplate.send(GmallConstants.KAFKA_TOPIC_EVENT, jsonObject.toString());
        }

    }
}
