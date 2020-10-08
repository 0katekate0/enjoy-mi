package cn.enjoy.users.service.impl;

import cn.enjoy.mall.model.UserAddress;
import cn.enjoy.mall.service.IUserAddressService;
import cn.enjoy.users.dao.UserAddressMapper;
import com.baidu.fsg.uid.impl.CachedUidGenerator;
import com.baidu.fsg.uid.impl.DefaultUidGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
/**
 * 用户地址管理
* @author Jack
* @date 2020/9/8
*/
@RestController
//@RequestMapping("/user/sys/service/IUserAddressService")
public class UserAddressServiceImpl implements IUserAddressService {
    @Resource
    private UserAddressMapper userAddressMapper;

    @Autowired
    private DefaultUidGenerator defaultUidGenerator;

    @Autowired
    private CachedUidGenerator cachedUidGenerator;

    /**
     * 保存用户地址
    * @param userAddress
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/save", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public void save(@RequestBody UserAddress userAddress) {
        if(userAddress.getAddressId()==null || userAddress.getAddressId() == 0){
            userAddress.setAddressId(defaultUidGenerator.getUID());
            userAddressMapper.insert(userAddress);
        }else{
            userAddressMapper.updateByPrimaryKey(userAddress);
        }
    }
    /**
     * 删除用户地址
    * @param addressId
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/remove", method = RequestMethod.POST)
    @Override
    public void remove(Integer addressId) {
        userAddressMapper.deleteByPrimaryKey(addressId);
    }

    /**
     * 查询用户地址
    * @param map
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/selectById", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public List<UserAddress> selectById(Map map) {
        return userAddressMapper.selectById(map);
    }
}
