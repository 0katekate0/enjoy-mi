package cn.enjoy;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.netflix.hystrix.dashboard.EnableHystrixDashboard;
import org.springframework.cloud.netflix.turbine.EnableTurbine;


/*
 * 监控界面：http://localhost:8194/hystrix
 * 需要监控的端点（使用了hystrix组件的端点）：http://localhost:8083/actuator/hystrix.stream
 *  http://localhost:8194/turbine.stream
 * */
@SpringBootApplication
@EnableTurbine
@EnableEurekaClient
@EnableHystrixDashboard
public class MallDashboardApplication {
    public static void main(String[] args) {
        SpringApplication.run(MallDashboardApplication.class,args);
    }
}
