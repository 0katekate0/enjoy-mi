package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.mall.dao.ShippingMapper;
import cn.enjoy.mall.model.Shipping;
import cn.enjoy.mall.service.manage.IShippingService;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 查询发货信息
 *
 * @author Jack
 * @date 2018/3/21.
 */
@RestController
//@RequestMapping("/order/mall/service/manage/IShippingService")
public class ShippingServiceImpl implements IShippingService {

    @Resource
    private ShippingMapper shippingMapper;

    /**
     * 查询发货信息
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryAll")
    @Override
    public List<Shipping> queryAll() {
        return shippingMapper.queryAll();
    }
}
