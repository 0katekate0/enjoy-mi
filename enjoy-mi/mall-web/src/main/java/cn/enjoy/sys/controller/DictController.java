package cn.enjoy.sys.controller;


import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.model.SysDictData;
import cn.enjoy.sys.service.IDictDataService;
import cn.enjoy.sys.util.DictCacheUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 字典数据管理
 *
 * @author Jack
 * @version 1.0
 */
@RestController
@RequestMapping(value = "/api/dict")
public class DictController extends BaseController {

    @Autowired
    private IDictDataService dictDataService;

    /**
     * 查询字典数据
     *
     * @param type
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @RequestMapping(value = "/loadDictDataByType", method = {RequestMethod.GET, RequestMethod.POST})
    public HttpResponseBody<List<SelectModel>> loadDictDataByType(String type) {
        List<SelectModel> models = DictCacheUtil.getDictDataSelectModelByType(type);
        return HttpResponseBody.successResponse("查询成功", models);
    }

    /**
     * 根据字典数据types获取字典数据
     *
     * @param types
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @RequestMapping(value = "/getDictDataByTypes", method = {RequestMethod.GET, RequestMethod.POST})
    public HttpResponseBody<Map<String, List>> queryDictDataByTypes(String types) {
        if (StringUtils.isEmpty(types)) {
            return null;
        }
        Map<String, List> returnMap = new HashMap<String, List>();
        String[] typeStr = types.split(",");
        for (int i = 0; i < typeStr.length; i++) {
            List<SysDictData> dataList = DictCacheUtil.getDictDatasByType(typeStr[i]);
            if (dataList == null || dataList.isEmpty()) {
                dataList = dictDataService.findDictByType(typeStr[i]);
            }
            returnMap.put(typeStr[i], dataList);
        }
        return HttpResponseBody.successResponse("查询成功", returnMap);
    }

    /**
     * 根据字典数据type获取字典数据
     *
     * @param type
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @RequestMapping(value = "/getDictDataByType", method = {RequestMethod.GET, RequestMethod.POST})
    public HttpResponseBody<List<SysDictData>> queryDictDataByType(@RequestParam String type) {
        if (StringUtils.isEmpty(type)) {
            return null;
        }
        List<SysDictData> dataList = DictCacheUtil.getDictDatasByType(type);
        if (dataList == null || dataList.isEmpty()) {
            dataList = dictDataService.findDictByType(type);
        }
        return HttpResponseBody.successResponse("查询成功", dataList);
    }

    /**
     * 全量查询字典数据
     *
     * @param param
     * @param type
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
    @RequestMapping(value = {"queryAllDictDataPage"}, method = {RequestMethod.GET, RequestMethod.POST})
    public HttpResponseBody<GridModel<SysDictData>> queryAllDictTypePage(@RequestParam(required = false, defaultValue = "") String param, @RequestParam(required = false, defaultValue = "") String type, @RequestParam(required = false, defaultValue = "0") int page,
                                                                         @RequestParam(required = false, defaultValue = "10") int pageSize, @RequestParam(required = false, defaultValue = "") String sidx, @RequestParam(required = false, defaultValue = "") String sord) {
        GridModel<SysDictData> SysDictGridModel = dictDataService.queryAllDictDataPage(param, type, page, pageSize, sidx, sord);
        return HttpResponseBody.successResponse("查询成功", SysDictGridModel);
    }

    /**
     * 批量删除字典数据
     *
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("batchDeleteDictData")
    public HttpResponseBody batchDeleteDictData(@RequestParam(name = "ids") String[] ids) {
        dictDataService.deleteByIds(Arrays.asList(ids));
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 新增字典数据
     *
     * @param SysDictData
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @RequestMapping(value = "addDictData", method = RequestMethod.POST)
    public HttpResponseBody addDictData(SysDictData SysDictData) {
        dictDataService.addDictData(SysDictData);
        return HttpResponseBody.successResponse("新增成功");
    }

    /**
     * 查询字典详情
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @RequestMapping(value = {"detailDictData"}, method = {RequestMethod.GET, RequestMethod.POST})
    public HttpResponseBody<Map<String, Object>> detailDictData(String id) {
        SysDictData sysDictData = dictDataService.selectByPrimaryKey(id);
        Map<String, Object> result = new HashMap<>();
        result.put("sysDictData", sysDictData);
        return HttpResponseBody.successResponse("查询成功", result);
    }

    /**
     * 修改字典数据
     *
     * @param sysDictData
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("updateDictData")
    public HttpResponseBody updateDictData(SysDictData sysDictData) {
        dictDataService.updateDictData(sysDictData);
        return HttpResponseBody.successResponse("修改成功");
    }

    /**
     * 删除字典数据
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("deleteDictData")
    public HttpResponseBody deleteDictData(String id) {
        dictDataService.deleteDictData(id);
        return HttpResponseBody.successResponse("删除成功");
    }

    /**
     * 冻结字典数据
     *
     * @param id
     * @param status
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @PostMapping("freezeDictData")
    public HttpResponseBody freezeDictData(String id, String status) {
        dictDataService.freezeDictData(id, status);
        return HttpResponseBody.successResponse("操作成功");
    }
}

