package com.real.utils;

import java.util.Date;

public class RandomDate {
    Long logDateTime = 0L;
    int maxTimeStep = 0;

    public RandomDate(Date start, Date end, int num) {
        Long avgStepTime = (end.getTime() - start.getTime()) / num;
        this.maxTimeStep = avgStepTime.intValue() * 2;
        this.logDateTime = start.getTime();
    }
}
