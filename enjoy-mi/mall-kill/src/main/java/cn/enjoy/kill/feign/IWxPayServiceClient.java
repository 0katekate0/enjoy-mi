package cn.enjoy.kill.feign;

import cn.enjoy.mall.service.IWxPayService;
import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = /*"MALL-PAY-SERVICE"*/"API-GATEWAY"/*,path = "/pay"*/)
public interface IWxPayServiceClient extends IWxPayService {
}
