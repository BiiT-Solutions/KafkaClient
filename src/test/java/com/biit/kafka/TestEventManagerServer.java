package com.biit.kafka;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Service;

@SpringBootApplication
@ComponentScan({"com.biit.kafka", "com.biit.cipher"})
@PropertySource("classpath:application.properties")
@Service
public class TestEventManagerServer {

    public static void main(String[] args) {
        SpringApplication.run(TestEventManagerServer.class, args);
    }

}
