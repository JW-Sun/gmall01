package com.real.service.impl;

import com.real.bean.OrderHourAmount;
import com.real.mapper.OrderMapper;
import com.real.service.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderServiceImpl implements OrderService {

    @Autowired
    private OrderMapper orderMapper;

    @Override
    public Double selectOrderAmountTotal(String date) {
        return orderMapper.selectOrderAmountTotal(date);
    }

    @Override
    public Map<String, Double> getOrderHourAmount(String date) {

        //将List集合转换成Map
        List<OrderHourAmount> orderHourAmounts = orderMapper.selectOrderAmountHourMap(date);

        Map<String, Double> map = new HashMap<>();

        for (OrderHourAmount hourAmount : orderHourAmounts) {
            map.put(hourAmount.getCreateHour(), hourAmount.getSumOrderAmount());
        }

        return map;
    }
}
