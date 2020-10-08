package cn.enjoy.sys.feign;

import cn.enjoy.sys.service.IResourceService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-USER-SERVICE"*/"API-GATEWAY"/*,path = "/user"*/)
public interface IResourceServiceClient extends IResourceService {
}
