package com.real.service;

import java.util.Map;

public interface PublishService {

    /***
     *根据日期查询这个日期的所有新增个数
     * @param date
     * @return
     */
    Long getDauTotal(String date);

    /***
     * 这个日期中每个小时的新增人数
     * @param date
     * @return
     */
    Map<String, Long> getDauHourCount(String date);



}
