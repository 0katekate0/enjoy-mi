package cn.enjoy.mall.feign;

import cn.enjoy.mall.service.manage.IKillOrderManageService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-ORDER-SERVICE"*/"API-GATEWAY"/*,path = "/order"*/)
public interface IKillOrderManageServiceClient extends IKillOrderManageService {
}
