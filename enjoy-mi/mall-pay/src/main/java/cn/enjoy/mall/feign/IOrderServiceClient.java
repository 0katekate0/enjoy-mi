package cn.enjoy.mall.feign;

import cn.enjoy.mall.service.IOrderService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-ORDER-SERVICE"*/"API-GATEWAY"/*,path = "/order"*/)
public interface IOrderServiceClient extends IOrderService {
}
