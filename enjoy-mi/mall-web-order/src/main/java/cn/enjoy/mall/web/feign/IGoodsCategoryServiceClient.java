package cn.enjoy.mall.web.feign;

import cn.enjoy.mall.service.IGoodsCategoryService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-PRODUCT-SERVICE"*/"API-GATEWAY"/*,path = "/product"*/)
public interface IGoodsCategoryServiceClient extends IGoodsCategoryService {
}
