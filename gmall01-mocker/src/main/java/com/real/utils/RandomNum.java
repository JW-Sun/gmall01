package com.real.utils;

import java.util.Random;

public class RandomNum {
    public static final int getRandInt(int from, int to) {
        return from + new Random().nextInt(to - from + 1);
    }
}
