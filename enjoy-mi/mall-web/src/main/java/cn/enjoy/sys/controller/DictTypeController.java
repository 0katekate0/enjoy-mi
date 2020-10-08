package cn.enjoy.sys.controller;


import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.sys.model.SysDictType;
import cn.enjoy.sys.service.IDictTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * 字典表类型管理
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
@RequestMapping(value = "/api/dictType")
public class DictTypeController extends BaseController {

    @Autowired
    private IDictTypeService dictTypeService;

    /**
     * 查询字典类型列表
     *
     * @param param
     * @param page
     * @param pageSize
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @GetMapping("queryAllDictTypePage")
    public HttpResponseBody<GridModel<SysDictType>> queryAllDictTypePage(@RequestParam(required = false, defaultValue = "") String param, @RequestParam(required = false, defaultValue = "0") int page,
                                                                         @RequestParam(required = false, defaultValue = "10") int pageSize, @RequestParam(required = false, defaultValue = "") String sidx, @RequestParam(required = false, defaultValue = "") String sord) {
        GridModel<SysDictType> SysDictGridModel = dictTypeService.queryAllDictTypePage(param, page, pageSize, sidx, sord);
        return HttpResponseBody.successResponse("查询成功", SysDictGridModel);
    }

    /**
     * 删除字典类型
     *
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("batchDeleteDictType")
    public HttpResponseBody batchDeleteDictType(@RequestParam(name = "ids") String[] ids) {
        dictTypeService.deleteByIds(Arrays.asList(ids));
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 新增字典类型
     *
     * @param sysDictType
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("addDictType")
    public HttpResponseBody addDictType(SysDictType sysDictType) {
        dictTypeService.addDictType(sysDictType);
        return HttpResponseBody.successResponse("新增成功");
    }

    /**
     * 查询字典类型详情
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @GetMapping("detailDictType")
    public HttpResponseBody detailDictType(String id) {
        SysDictType sysDictType = dictTypeService.selectByPrimaryKey(id);
        Map<String, Object> result = new HashMap();
        result.put("SysDictType", sysDictType);
        return HttpResponseBody.successResponse("查询成功", result);
    }

    /**
     * 删除字典类型
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("deleteDictType")
    public HttpResponseBody deleteDictType(String id) {
        dictTypeService.deleteDictType(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 修改字典类型
     *
     * @param sysDictType
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("updateDictType")
    public HttpResponseBody updateDictType(SysDictType sysDictType) {
        dictTypeService.updateDictType(sysDictType);
        return HttpResponseBody.successResponse("修改成功");
    }

    /**
     * 冻结字典类型
     *
     * @param id
     * @param status
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("freezeDictType")
    public HttpResponseBody freezeDictType(String id, String status) {
        dictTypeService.freezeDictTyp(id, status);
        return HttpResponseBody.successResponse("操作成功");
    }
}

