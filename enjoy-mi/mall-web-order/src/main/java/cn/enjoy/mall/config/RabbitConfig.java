package cn.enjoy.mall.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 *类说明：消息队列配置
 */
@Slf4j
@Configuration
public class RabbitConfig {

    public final static String EXCHANGE_SECKILL = "order.seckill.producer";
    public final static String KEY_SECKILL = "order.seckill";

    @Value("${spring.rabbitmq.host}")
    private String addresses;

    @Value("${spring.rabbitmq.port}")
    private String port;

    @Value("${spring.rabbitmq.username}")
    private String username;

    @Value("${spring.rabbitmq.password}")
    private String password;

    @Value("${spring.rabbitmq.virtual-host}")
    private String virtualHost;

    @Value("${spring.rabbitmq.publisher-confirms}")
    private boolean publisherConfirms;

    @Bean
    public ConnectionFactory connectionFactory() {

        CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
        connectionFactory.setAddresses(addresses+":"+port);
        connectionFactory.setUsername(username);
        connectionFactory.setPassword(password);
        connectionFactory.setVirtualHost(virtualHost);
        /** 如果要进行消息回调，则这里必须要设置为true */
//        connectionFactory.setPublisherConfirms(publisherConfirms);
        return connectionFactory;
    }

    @Bean
    public RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory){
        return new RabbitAdmin(connectionFactory);
    }

    @Bean
    public RabbitTemplate newRabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(connectionFactory());
//        template.setMandatory(true);
//        template.setConfirmCallback(confirmCallback());
//        template.setReturnCallback(returnCallback());
        //不使用临时队列
        template.setUseTemporaryReplyQueues(false);
        template.setReplyAddress("amq.rabbitmq.reply-to");
        template.setUserCorrelationId(true);
        template.setReplyTimeout(10000);
        return template;
    }


    @Bean
    public Queue queueSecKillMessage() {
        return new Queue("order.seckill.producer",true,false,false);
    }

    @Bean
    public DirectExchange exchange() {
        return new DirectExchange(EXCHANGE_SECKILL,true,false);
    }

    @Bean
    public Binding bindingSecKillExchangeMessage() {
        return BindingBuilder
                .bind(queueSecKillMessage())
                .to(exchange())
                .with(KEY_SECKILL);
    }


    //===============生产者发送确认==========
    @Bean
    public RabbitTemplate.ConfirmCallback confirmCallback(){
        return new RabbitTemplate.ConfirmCallback(){
            @Override
            public void confirm(CorrelationData correlationData,
                                boolean ack, String cause) {
                if (ack) {
                    log.info("发送者确认发送给mq成功");
                } else {
                    //处理失败的消息
                    log.info("发送者发送给mq失败,考虑重发:"+cause);
                }
            }
        };
    }

    @Bean
    public RabbitTemplate.ReturnCallback returnCallback(){
        return new RabbitTemplate.ReturnCallback(){
            @Override
            public void returnedMessage(Message message,
                                        int replyCode,
                                        String replyText,
                                        String exchange,
                                        String routingKey) {
                log.info("无法路由的消息，需要考虑另外处理。");
                log.info("Returned replyText："+replyText);
                log.info("Returned exchange："+exchange);
                log.info("Returned routingKey："+routingKey);
                String msgJson  = new String(message.getBody());
                log.info("Returned Message："+msgJson);
            }
        };
    }

}
