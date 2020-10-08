package cn.enjoy.sys.feign;

import cn.enjoy.sys.service.ILoginService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-USER-SERVICE"*/"API-GATEWAY"/*,path = "/user"*/)
public interface ILoginServiceClient extends ILoginService {
}
