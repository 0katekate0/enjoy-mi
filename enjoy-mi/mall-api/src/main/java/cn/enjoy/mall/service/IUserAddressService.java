package cn.enjoy.mall.service;

import cn.enjoy.mall.model.UserAddress;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@RequestMapping("/user/sys/service/IUserAddressService")
public interface IUserAddressService {
    @RequestMapping(value = "/save", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    void save(@RequestBody UserAddress userAddress);

    @RequestMapping(value = "/remove", method = RequestMethod.POST)
    void remove(@RequestParam("addressId") Integer addressId);

    @RequestMapping(value = "/selectById", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    List<UserAddress> selectById(@RequestBody Map map);
}
