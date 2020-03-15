package com.real.service;

import java.util.List;
import java.util.Map;

public interface OrderService {

    //一天所有的销售值
    Double selectOrderAmountTotal(String date);

    //一天中分时的销售总额
    Map<String, Double> getOrderHourAmount(String date);

}
