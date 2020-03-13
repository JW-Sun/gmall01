package com.real;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.real.mapper")
public class Gmall01PublishApplication {
    public static void main(String[] args) {
        SpringApplication.run(Gmall01PublishApplication.class, args);
    }
}
