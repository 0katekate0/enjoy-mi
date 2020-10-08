package cn.enjoy.mall.service.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.GoodsAttribute;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Ray
 * @date 2018/3/7.
 */
@RequestMapping("/product/mall/service/manage/IAttributeManageService")
public interface IAttributeManageService {

    /**
     * 分页查询
     * @param page
     * @param pageSize
     * @param attribute
     * @return
     */
    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<GoodsAttribute> queryByPage(@RequestParam(value = "page") int page,
                                          @RequestParam(value = "pageSize") int pageSize,
                                          @RequestBody GoodsAttribute attribute);

    /**
     * 保存属性和属性项
     * @param spec Spec
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestBody GoodsAttribute spec);


    /**
     * 更新属性
     * @param spec Spec
     */
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    void update(@RequestBody GoodsAttribute spec);

    /**
     * 删除属性
     * @param id
     */
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    void delete(@RequestParam("id") int id);

    /**
     * 批量删除
     * @param ids
     */
    @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    void deleteByIds(@RequestBody String[] ids);
}
