package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.dao.SpecItemMapper;
import cn.enjoy.mall.dao.SpecMapper;
import cn.enjoy.mall.model.Spec;
import cn.enjoy.mall.model.SpecItem;
import cn.enjoy.mall.service.manage.IGoodsSpecManageService;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.miemiedev.mybatis.paginator.domain.Paginator;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

/**
 * 商品模型
 *
 * @author Jack
 * @date 2018/3/12.
 */
@RestController
//@RequestMapping("/product/mall/service/manage/IGoodsSpecManageService")
public class GoodsSpecManageServiceImpl implements IGoodsSpecManageService {

    @Resource
    private SpecMapper specMapper;
    @Resource
    private SpecItemMapper specItemMapper;

    /**
     * 查询商品模型列表
     *
     * @param page
     * @param pageSize
     * @param name
     * @param typeId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<Spec> queryByPage(int page, int pageSize, String name, Integer typeId) {
        PageBounds pageBounds = new PageBounds(page, pageSize);
        PageList<Spec> list = specMapper.queryByPage(name, typeId, pageBounds);

        if (list.getPaginator() == null) {
            Integer totalCount = specMapper.queryByPageTotalCount(name, typeId);
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            list = new PageList<>(list, paginator);
//            GridModel<Spec> list2Model = new GridModel<>(list2);
//            return list2Model;
        }


        for (Spec spec : list) {
            //4.1 SpecItem
            List<SpecItem> itemList = specItemMapper.selectListBySpecId(spec.getId());
            spec.setSpecItemList(itemList);
        }
        return new GridModel<>(list);
    }

    /**
     * 保存规格和规格项
     *
     * @param spec
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public void save(Spec spec) {
        if (spec.getId() == null) {
            specMapper.insert(spec);
        } else {
            specItemMapper.deleteBySpecId(spec.getId());
            specMapper.updateByPrimaryKeySelective(spec);
        }
        //写specItem
        for (SpecItem item : spec.getSpecItemList()) {
            item.setSpecId(spec.getId());
            specItemMapper.insertSelective(item);
        }
    }

    /**
     * 更新规格
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
    public void update(Spec spec) {
        specMapper.updateByPrimaryKeySelective(spec);
    }

    /**
     * 删除规格
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
        specMapper.deleteByPrimaryKey(id);
    }

    /**
     * 批量删除规格
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
            specMapper.deleteByPrimaryKey(Integer.parseInt(id));
        }
    }
}
