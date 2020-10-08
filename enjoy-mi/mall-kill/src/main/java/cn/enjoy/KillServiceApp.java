package cn.enjoy;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.JdbcTemplateAutoConfiguration;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.context.IntegrationContextUtils;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * @Classname KillServiceApp
 * @Description 专门做秒杀业务的服务，跟其他服务做隔离
 * @Author Jack
 * Date 2020/8/12 13:58
 * Version 1.0
 */
@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, JdbcTemplateAutoConfiguration.class})
@MapperScan("cn.enjoy.kill.dao")
@EnableScheduling
@EnableEurekaClient
@EnableCircuitBreaker
@EnableFeignClients(basePackages = {"cn.enjoy.kill.feign"})
public class KillServiceApp {
    public static void main(String[] args) {
        SpringApplication.run(KillServiceApp.class, args);
    }

    @Bean(name = IntegrationContextUtils.INTEGRATION_DATATYPE_CHANNEL_MESSAGE_CONVERTER_BEAN_NAME)
    MappingJackson2MessageConverter mappingJackson2MessageConverter(ObjectMapper objectMapper) {
        MappingJackson2MessageConverter converter = new MappingJackson2MessageConverter();
        converter.setObjectMapper(objectMapper);
        return converter;
    }
}
