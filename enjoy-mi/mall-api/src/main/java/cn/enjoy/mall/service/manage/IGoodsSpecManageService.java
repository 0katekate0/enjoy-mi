package cn.enjoy.mall.service.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.model.Spec;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * @author Ray
 * @date 2018/3/7.
 */
@RequestMapping("/product/mall/service/manage/IGoodsSpecManageService")
public interface IGoodsSpecManageService {

    /**
     * 分页查询
     * @param page
     * @param pageSize
     * @param name
     * @return
     */
    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<Spec> queryByPage(@RequestParam(value = "page",required = false) int page, @RequestParam(value = "pageSize",required = false) int pageSize,
                                @RequestParam(value = "name",required = false) String name, @RequestParam(value = "typeId",required = false) Integer typeId);

    /**
     * 保存规格和规格项
     * @param spec Spec
     */
    @RequestMapping(value = "/save", method = RequestMethod.POST)
    void save(@RequestBody Spec spec);


    /**
     * 更新规格
     * @param spec Spec
     */
    @RequestMapping(value = "/update", method = RequestMethod.POST)
    void update(@RequestBody Spec spec);

    /**
     * 删除规格
     * @param id
     */
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    void delete(@RequestParam("id") int id);

    /**
     * 批量删除规格
     * @param ids
     */
    @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    void deleteByIds(@RequestBody String[] ids);
}
