package com.real.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomOptionGroup<T> {
    int totalWeight = 0;

    List<RanOpt> optList = new ArrayList<>();

    public RandomOptionGroup(RanOpt<T>...opts) {
        for (RanOpt<T> opt : opts) {
            totalWeight += opt.getWeight();
            for (int i = 0; i < opt.getWeight(); i++) {
                optList.add(opt);
            }
        }
    }

    public RanOpt<T> getRandomOpt() {
        int i = new Random().nextInt(totalWeight);
        return optList.get(i);
    }

    public static void main(String[] args) {
        RanOpt[] opts = {
                new RanOpt("aaa", 134),
                new RanOpt("bbb", 22),
                new RanOpt("ccc", 36)};

        RandomOptionGroup randomOptionGroup = new RandomOptionGroup(opts);
        for (int i = 0; i < 10; i++) {
            System.out.println(randomOptionGroup.getRandomOpt().getValue());
        }
    }
}
