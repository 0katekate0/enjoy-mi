package cn.enjoy;

import cn.enjoy.config.ShiroOauth2Configuration;
import com.github.tobato.fastdfs.FdfsClientConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.circuitbreaker.EnableCircuitBreaker;
import org.springframework.cloud.netflix.eureka.EnableEurekaClient;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.EnableMBeanExport;
import org.springframework.context.annotation.Import;
import org.springframework.jmx.support.RegistrationPolicy;

/**
 * @author Ray
 * @date 2018/2/1.
 */
@SpringBootApplication
@Import({ShiroOauth2Configuration.class, FdfsClientConfig.class})
@EnableFeignClients(basePackages = {"cn.enjoy.mall.web.feign","cn.enjoy.sys.feign"})
@EnableEurekaClient
@EnableCircuitBreaker
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
public class FrontApp {

    public static void main(String[] args) {
         SpringApplication.run(FrontApp.class, args);
    }

}
