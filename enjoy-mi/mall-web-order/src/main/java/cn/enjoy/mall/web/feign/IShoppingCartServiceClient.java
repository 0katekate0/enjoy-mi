package cn.enjoy.mall.web.feign;

import cn.enjoy.mall.service.IShoppingCartService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-ORDER-SERVICE"*/"API-GATEWAY"/*,path = "/order"*/)
public interface IShoppingCartServiceClient extends IShoppingCartService {
}
