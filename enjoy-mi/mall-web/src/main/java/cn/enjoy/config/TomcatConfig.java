package cn.enjoy.config;

import org.apache.catalina.connector.Connector;
import org.apache.coyote.http11.Http11NioProtocol;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;

/**
 * @Classname TomcatConfig
 * @Description 设置tomcat的配置
 * @Author Jack
 * Date 2020/8/6 15:50
 * Version 1.0
 */
//@Configuration
public class TomcatConfig {
    @Bean
    public ConfigurableServletWebServerFactory configurableServletWebServerFactory() {
        TomcatServletWebServerFactory tomcat = new TomcatServletWebServerFactory();
        tomcat.addAdditionalTomcatConnectors(createConnector());
        return tomcat;
    }

    private Connector createConnector() {
        Connector connector = new Connector("org.apache.coyote.http11.Http11NioProtocol");
        Http11NioProtocol protocol = (Http11NioProtocol) connector.getProtocolHandler();
        connector.setPort(8090);
        // 最大线程数
        protocol.setMaxThreads(2);
        // 最大连接数
        protocol.setMaxConnections(10);
        return connector;
    }
}
