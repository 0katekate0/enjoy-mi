package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.dao.GoodsCategoryMapper;
import cn.enjoy.mall.model.GoodsCategory;
import cn.enjoy.mall.service.manage.ICategoryManageService;
import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.miemiedev.mybatis.paginator.domain.Paginator;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 商品目录管理
 *
 * @author Jack
 * @date 2018/3/12.
 */
@RestController
//@RequestMapping("/product/mall/service/manage/ICategoryManageService")
public class CategoryManageServiceImpl implements ICategoryManageService {

    @Resource
    private GoodsCategoryMapper goodsCategoryMapper;

    /**
     * 查询商品目录列表
     *
     * @param page
     * @param pageSize
     * @param parentId
     * @param categoryName
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<GoodsCategory> queryByPage(int page, int pageSize, String parentId, String categoryName) {
        PageBounds pageBounds = new PageBounds(page, pageSize, Order.formString("sort_order.asc"));
        PageList<GoodsCategory> goodsCategories = goodsCategoryMapper.queryByPage(parentId, categoryName, pageBounds);

        if (goodsCategories.getPaginator() == null) {
            Integer totalCount = goodsCategoryMapper.queryByPageCount(parentId, categoryName);
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            goodsCategories = new PageList<>(goodsCategories, paginator);
            return new GridModel<>(goodsCategories);
        }

        return new GridModel<>(goodsCategories);
    }

    /**
     * 保存货更新商品目录
     *
     * @param goodsCategory
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public void save(GoodsCategory goodsCategory) {
        if (goodsCategory.getId() == null) {
            goodsCategoryMapper.insert(goodsCategory);
            //更新parent_id_path,因为这个值是根据本分类ID来生成的
            goodsCategory.setParentIdPath(goodsCategory.getParentIdPath() + "_" + goodsCategory.getId());
        }
        goodsCategoryMapper.updateByPrimaryKeySelective(goodsCategory);
    }

    /**
     * 删除商品目录
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/delete", method = RequestMethod.POST)
    @Override
    public void delete(short id) {
        goodsCategoryMapper.deleteByPrimaryKey(id);
    }

    /**
     * 批量删除商品目录
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
            goodsCategoryMapper.deleteByPrimaryKey(Short.parseShort(id));
        }
    }
}
