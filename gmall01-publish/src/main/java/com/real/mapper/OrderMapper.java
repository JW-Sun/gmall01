package com.real.mapper;

import com.real.bean.OrderHourAmount;

import java.util.List;
import java.util.Map;

public interface OrderMapper {

    /***
     * 查询当日交易额总数
     * @param date
     * @return
     */
    Double selectOrderAmountTotal(String date);


    /***
     * 查询交易当日交易额分时明细
     * @param date
     * @return
     */
    List<OrderHourAmount> selectOrderAmountHourMap(String date);

}
