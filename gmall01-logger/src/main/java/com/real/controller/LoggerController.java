package com.real.controller;

import com.real.service.LoggerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


@RestController
public class LoggerController {

    @Autowired
    private LoggerService loggerService;

    @PostMapping("/log")
    public String getLogToKafka(@RequestParam("log") String logJson) {

        loggerService.createLog(logJson);

        return "logJson";
    }

}
