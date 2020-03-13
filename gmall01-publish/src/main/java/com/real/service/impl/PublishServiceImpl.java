package com.real.service.impl;

import com.real.mapper.DauMapper;
import com.real.service.PublishService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class PublishServiceImpl implements PublishService {

    @Autowired
    private DauMapper dauMapper;

    @Override
    public Long getDauTotal(String date) {
        return dauMapper.getDauTotal(date);
    }

    @Override
    public Map<String, Long> getDauHourCount(String date) {
        List<Map> dauHourCountList = dauMapper.getDauHourCount(date);
        Map<String, Long> hourMap = new HashMap<>();

        for (Map map : dauHourCountList) {
            hourMap.put((String)map.get("LOGHOUR"), (Long)map.get("CT"));
        }
        return hourMap;
    }
}
