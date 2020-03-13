package com.real.mapper;

import java.util.List;
import java.util.Map;

public interface DauMapper {

    /***
     * 按照日期查询hbase
     * @param date
     * @return
     */
    Long getDauTotal(String date);

    /***
     * 根据日期查询每个小时的数量
     * @param date
     * @return
     */
    List<Map> getDauHourCount(String date);

}
