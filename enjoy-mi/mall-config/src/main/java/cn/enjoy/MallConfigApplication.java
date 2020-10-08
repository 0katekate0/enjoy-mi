package cn.enjoy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.config.server.EnableConfigServer;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;

/*
post
* 加密：http://localhost:8085/encrypt?data=123456
* 解密：http://localhost:8085/decrypt
* */
@SpringBootApplication
@EnableConfigServer
// 注册到eureka
@EnableEurekaClient
public class MallConfigApplication {
    public static void main(String[] args) {
        SpringApplication.run(MallConfigApplication.class,args);
    }
}
