package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.mall.dao.BrandMapper;
import cn.enjoy.mall.model.Brand;
import cn.enjoy.mall.service.manage.IBrandService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * 商品品牌
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/product/mall/service/manage/IBrandService")
public class BrandServiceImpl implements IBrandService {
    @Autowired
    private BrandMapper brandMapper;

    /**
     * 查询全量品牌信息
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/getAll")
    @Override
    public List<Brand> getAll() {
        return brandMapper.selectAll();
    }
}
