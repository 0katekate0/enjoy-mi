package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.dao.GoodsTypeMapper;
import cn.enjoy.mall.model.GoodsType;
import cn.enjoy.mall.service.manage.IGoodsTypeManageService;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.miemiedev.mybatis.paginator.domain.Paginator;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 商品类型
 *
 * @author Jack
 * @date 2018/3/12.
 */
@RestController
//@RequestMapping("/product/mall/service/manage/IGoodsTypeManageService")
public class GoodsTypeManageServiceImpl implements IGoodsTypeManageService {

    @Resource
    private GoodsTypeMapper goodsTypeMapper;

    /**
     * 查询商品类型列表
     *
     * @param page
     * @param pageSize
     * @param parentId
     * @param name
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<GoodsType> queryByPage(int page, int pageSize, String parentId, String name) {
        PageBounds pageBounds = new PageBounds(page, pageSize);

        PageList<GoodsType> goodsTypes = goodsTypeMapper.queryByPage(name, pageBounds);

        if (goodsTypes.getPaginator() == null) {
            Integer totalCount = goodsTypeMapper.queryByPageTotalCount(name);
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            PageList<GoodsType> goodsTypes2 = new PageList<>(goodsTypes, paginator);
            GridModel<GoodsType> goodsTypesModel = new GridModel<>(goodsTypes2);
            return goodsTypesModel;
        }

        return new GridModel<>(goodsTypes);
    }

    /**
     * 全量查询商品类型
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryAll")
    @Override
    public List<GoodsType> queryAll() {
        return goodsTypeMapper.queryAll();
    }

    /**
     * 保存或者更新商品类型
     *
     * @param goodsType
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public void save(GoodsType goodsType) {
        if (goodsType.getId() == null) {
            goodsTypeMapper.insert(goodsType);
        } else {
            goodsTypeMapper.updateByPrimaryKeySelective(goodsType);
        }
    }

    /**
     * 删除商品类型
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
   // @RequestMapping(value = "/delete", method = RequestMethod.POST)
    @Override
    public void delete(short id) {
        goodsTypeMapper.deleteByPrimaryKey(id);
    }

    /**
     * 批量删除商品类型
     *
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    @Override
    public void deleteByIds(String[] ids) {
        for (String id : ids) {
            goodsTypeMapper.deleteByPrimaryKey(Short.parseShort(id));
        }
    }
}
