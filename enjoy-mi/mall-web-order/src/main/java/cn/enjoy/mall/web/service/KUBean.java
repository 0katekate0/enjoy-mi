package cn.enjoy.mall.web.service;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;

/**
 * @Classname KUBean
 * @Description TODO
 * @Author Jack
 * Date 2020/9/16 16:07
 * Version 1.0
 */
@Getter
@Setter
@AllArgsConstructor
public class KUBean {
    // 秒杀id
    private Integer killId;
    // userId
    private String userId;
}
