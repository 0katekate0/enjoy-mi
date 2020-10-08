package cn.enjoy.mall.config;

import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;

/**
 * @Classname RedssonConfig
 * @Description TODO
 * @Author Jack
 * Date 2020/9/16 20:47
 * Version 1.0
 */
@Configuration
public class RedssonConfig {

    @Bean(name = "redissonClient",destroyMethod = "shutdown")
    public RedissonClient redissonClient() throws IOException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://106.52.62.38:6379");
        config.useSingleServer().setPassword("123456");
        config.useSingleServer().setConnectionPoolSize(1000);
        config.useSingleServer().setConnectionMinimumIdleSize(100);
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }

    @Bean(name = "redissonClient1",destroyMethod = "shutdown")
    public RedissonClient redissonClient1() throws IOException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://106.52.62.38:6379");
        config.useSingleServer().setPassword("123456");
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }

    @Bean(name = "redissonClient2",destroyMethod = "shutdown")
    public RedissonClient redissonClient2() throws IOException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://106.52.62.38:6380");
        config.useSingleServer().setPassword("123456");
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }

    @Bean(name = "redissonClient3",destroyMethod = "shutdown")
    public RedissonClient redissonClient3() throws IOException {
        Config config = new Config();
        config.useSingleServer().setAddress("redis://106.52.62.38:6381");
        config.useSingleServer().setPassword("123456");
        RedissonClient redisson = Redisson.create(config);
        return redisson;
    }
}
