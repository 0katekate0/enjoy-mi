package cn.enjoy.sys.feign;

import cn.enjoy.sys.service.IDictDataService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-USER-SERVICE"*/"API-GATEWAY"/*,path = "/user"*/)
public interface IDictDataServiceClient extends IDictDataService {
}
