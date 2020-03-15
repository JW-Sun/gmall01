package com.real.controller;

import com.alibaba.fastjson.JSON;
import com.real.service.OrderService;
import com.real.service.PublishService;
import org.apache.commons.lang3.time.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

@RestController
public class PublishController {

    @Autowired
    private PublishService publishService;

    @Autowired
    private OrderService orderService;

    //realtime-total?date=2020-03-13
    @GetMapping("realtime-total")
    public String getTotal(@RequestParam("date") String date) {
        Long dauTotal = publishService.getDauTotal(date);

        List<Map<String, Object>> totalList = new ArrayList<>();

        Map<String, Object> dauMap = new HashMap<>();
        dauMap.put("id", "dau");
        dauMap.put("name", "新增日活");
        dauMap.put("value", dauTotal);

        totalList.add(dauMap);

        Map<String, Object> orderAmountMap = new HashMap<>();
        dauMap.put("id", "order_amount");
        dauMap.put("name", "交易额");
        Double orderAmountTotal = orderService.selectOrderAmountTotal(date);
        dauMap.put("value", orderAmountTotal);

        totalList.add(orderAmountMap);

        String res = JSON.toJSONString(totalList);

        return res;
    }

    @GetMapping("realtime-hour")
    public String getRealTimeHour(@RequestParam("id") String id,
                                  @RequestParam("date") String date) {
        if ("dau".equals(id)) {
            //今天
            Map<String, Long> dauHourCount = publishService.getDauHourCount(date);

            //昨天
            String yesterday = getYesterday(date);
            Map<String, Long> dauHourCountYesterday = publishService.getDauHourCount(yesterday);

            Map hourMap = new HashMap();

            hourMap.put("today", dauHourCount);
            hourMap.put("yesterday", dauHourCountYesterday);

            return JSON.toJSONString(hourMap);
        } else if ("order_amount".equals(id)) {
            //今天
            Map<String, Double> orderHourAmount = orderService.getOrderHourAmount(date);
            //昨天
            String yesterday = getYesterday(date);

            Map<String, Double> hourMapYesterday = orderService.getOrderHourAmount(yesterday);

            Map hourAmountMap = new HashMap();

            hourAmountMap.put("today", orderHourAmount);
            hourAmountMap.put("yesterday", hourMapYesterday);

            return JSON.toJSONString(hourAmountMap);
        }

        return null;
    }

    private String getYesterday(String date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

        String yesterday = null;

        try {
            //将String转化成Date
            Date today = simpleDateFormat.parse(date);
            Date yester = DateUtils.addDays(today, -1);
            yesterday = simpleDateFormat.format(yester);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return yesterday;
    }


}
