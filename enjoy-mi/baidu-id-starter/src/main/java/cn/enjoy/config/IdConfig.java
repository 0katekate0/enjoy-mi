package cn.enjoy.config;

import com.baidu.fsg.uid.impl.CachedUidGenerator;
import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import com.baidu.fsg.uid.worker.DisposableWorkerIdAssigner;
import org.mybatis.spring.annotation.MapperScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Classname IdConfig
 * @Description TODO
 * @Author Jack
 * Date 2020/8/10 16:57
 * Version 1.0
 */
@Configuration
@MapperScan("com.baidu.fsg.uid.worker.dao")
public class IdConfig {

    @Bean
    public DisposableWorkerIdAssigner disposableWorkerIdAssigner() {
        return new DisposableWorkerIdAssigner();
    }

    @Bean(value = "defaultUidGenerator")
    public DefaultUidGenerator initDefaultUid(DisposableWorkerIdAssigner disposableWorkerIdAssigner){
        DefaultUidGenerator defaultUidGenerator = new DefaultUidGenerator();
        defaultUidGenerator.setWorkerIdAssigner(disposableWorkerIdAssigner);
        defaultUidGenerator.setTimeBits(29);
        defaultUidGenerator.setWorkerBits(21);
        defaultUidGenerator.setSeqBits(13);
        defaultUidGenerator.setEpochStr(LocalDate.now().format( DateTimeFormatter.ofPattern("yyyy-MM-dd")));
        return defaultUidGenerator;
    }

    @Bean(value ="cachedUidGenerator")
    public CachedUidGenerator initCachedUidGenerator(DisposableWorkerIdAssigner disposableWorkerIdAssigner){
        CachedUidGenerator cachedUidGenerator = new CachedUidGenerator();
        cachedUidGenerator.setWorkerIdAssigner(disposableWorkerIdAssigner);
        return cachedUidGenerator;
    }
}
