package cn.enjoy.mall.web.feign;

import cn.enjoy.mall.service.IKillOrderService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-ORDER-SERVICE"*/"API-GATEWAY"/*,path = "/kill"*/)
public interface IKillOrderServiceClient extends IKillOrderService {
}
