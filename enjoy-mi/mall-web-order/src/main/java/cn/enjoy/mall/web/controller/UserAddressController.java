package cn.enjoy.mall.web.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.model.UserAddress;
import cn.enjoy.mall.service.IUserAddressService;
import cn.enjoy.sys.controller.BaseController;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/*
* 用户地址服务
* */
@RestController
@RequestMapping("/api/userAddress")
public class UserAddressController extends BaseController {
    @Autowired
    private IUserAddressService userAddressService;

    /**
     * 保存用户地址
    * @param userAddress
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("save")
    public HttpResponseBody save(UserAddress userAddress){
        userAddress.setUserId(getSessionUserId());
        userAddressService.save(userAddress);
        return HttpResponseBody.successResponse("ok");
    }

    /**
     * 删除用户地址
    * @param addressId
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("remove")
    public HttpResponseBody remove(@RequestParam(name = "addressId", required = false) Integer addressId){
        userAddressService.remove(addressId);
        return HttpResponseBody.successResponse("ok");
    }

    /**
     * 查询用的地址列表
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @GetMapping("list")
    public HttpResponseBody list(){
        Map map = new HashMap();
        map.put("userId",getSessionUserId());
        return HttpResponseBody.successResponse("ok",userAddressService.selectById(map));
    }

}
