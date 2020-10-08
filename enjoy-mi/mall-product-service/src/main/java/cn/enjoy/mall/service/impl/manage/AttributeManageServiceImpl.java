package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.dao.GoodsAttributeMapper;
import cn.enjoy.mall.model.GoodsAttribute;
import cn.enjoy.mall.service.manage.IAttributeManageService;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.miemiedev.mybatis.paginator.domain.Paginator;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * 商品模型
 *
 * @author Jack
 * @date 2018/3/12.
 */
@RestController
//@RequestMapping("/product/mall/service/manage/IAttributeManageService")
public class AttributeManageServiceImpl implements IAttributeManageService {

    @Resource
    private GoodsAttributeMapper attributeMapper;

    /**
     * 查询商品属性列表
     *
     * @param page
     * @param pageSize
     * @param attribute
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<GoodsAttribute> queryByPage(int page, int pageSize, GoodsAttribute attribute) {
        PageBounds pageBounds = new PageBounds(page, pageSize);
        PageList<GoodsAttribute> list = attributeMapper.queryByPage(attribute, pageBounds);

        if (list.getPaginator() == null) {
            Integer totalCount = attributeMapper.queryByPageTotalCount(attribute);
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            list = new PageList<>(list, paginator);
            GridModel<GoodsAttribute> list2Model = new GridModel<>(list);
            return list2Model;
        }

        return new GridModel<>(list);
    }

    /**
     * 保存商品属性
     *
     * @param attribute
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public void save(GoodsAttribute attribute) {
        if (attribute.getAttrId() == null) {
            //不设1商品编辑看不到
            attribute.setAttrIndex((byte) 1);
            attributeMapper.insertSelective(attribute);
        } else {
            attributeMapper.updateByPrimaryKeySelective(attribute);
        }

    }

    /**
     * 更新商品属性
     *
     * @param spec
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/update", method = RequestMethod.POST)
    @Override
    public void update(GoodsAttribute spec) {
        attributeMapper.updateByPrimaryKeySelective(spec);
    }

    /**
     * 删除商品属性
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
    public void delete(int id) {
        attributeMapper.deleteByPrimaryKey(id);
    }

    /**
     * 批量删除商品属性
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
            attributeMapper.deleteByPrimaryKey(Integer.parseInt(id));
        }
    }
}
