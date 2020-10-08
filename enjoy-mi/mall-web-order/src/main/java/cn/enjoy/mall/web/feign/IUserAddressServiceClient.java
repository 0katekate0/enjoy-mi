package cn.enjoy.mall.web.feign;

import cn.enjoy.mall.service.IUserAddressService;
import org.springframework.cloud.openfeign.FeignClient;

/*
* API-GATEWAY
* */
@FeignClient(name = /*"MALL-ORDER-SERVICE"*/"API-GATEWAY"/*,path = "/order"*/)
public interface IUserAddressServiceClient extends IUserAddressService {
}
