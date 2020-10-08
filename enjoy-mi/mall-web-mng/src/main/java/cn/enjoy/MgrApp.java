package cn.enjoy;

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
@Import(FdfsClientConfig.class)
@EnableEurekaClient
@EnableCircuitBreaker
@EnableMBeanExport(registration = RegistrationPolicy.IGNORE_EXISTING)
@EnableFeignClients(basePackages = {"cn.enjoy"})
public class MgrApp {

    public static void main(String[] args) {
         SpringApplication.run(MgrApp.class, args);
    }
}
