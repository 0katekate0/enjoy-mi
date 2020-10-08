package cn.enjoy.mall.service.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.GoodsCategory;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Ray
 * @date 2018/3/7.
 */
@RequestMapping("/product/mall/service/manage/ICategoryManageService")
public interface ICategoryManageService {

    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<GoodsCategory> queryByPage(@RequestParam(value = "page",required = false) int page, @RequestParam(value = "pageSize",required = false) int pageSize,
                                         @RequestParam(value = "parentId",required = false) String parentId, @RequestParam(value = "categoryName",required = false) String categoryName);

    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestBody GoodsCategory goodsCategory);

    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    void delete(@RequestParam("id") short id);

    @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    void deleteByIds(@RequestBody String[] ids);
}
