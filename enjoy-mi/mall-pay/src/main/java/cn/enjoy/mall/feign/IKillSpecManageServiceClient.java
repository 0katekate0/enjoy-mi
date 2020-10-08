package cn.enjoy.mall.feign;

import cn.enjoy.mall.service.manage.IKillSpecManageService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-PRODUCT-SERVICE"*/"API-GATEWAY"/*,path = "/product"*/)
public interface IKillSpecManageServiceClient extends IKillSpecManageService {
}
