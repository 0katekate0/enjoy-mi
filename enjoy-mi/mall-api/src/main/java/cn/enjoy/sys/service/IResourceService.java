/**
 * Created on 2015年9月14日 by Caiming
 */
package cn.enjoy.sys.service;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.sys.model.ResourceTree;
import cn.enjoy.sys.model.SysResource;
import cn.enjoy.sys.model.TreeViewResource;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@RequestMapping("/user/sys/service/IResourceService")
public interface IResourceService {

    @RequestMapping(value = "/getMenuIdByUrl", method = RequestMethod.POST)
    String getMenuIdByUrl(@RequestParam("url") String url);

    /***
     * 获取资源树
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月28日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @return
     */
    @RequestMapping(value = "/queryResourceTree", method = RequestMethod.POST)
    List<ResourceTree> queryResourceTree(@RequestParam("roleId") String roleId, @RequestParam("parentId") String parentId);

    @RequestMapping(value = "/queryInitResourceTree", method = RequestMethod.POST)
    List<ResourceTree> queryInitResourceTree(@RequestParam("roleId") String roleId, @RequestParam("parentId") String parentId);

    /***
     * 获取资源树
     * <li>创建人：maojia</li>
     * <li>创建时间：2015年10月28日</li>
     * <li>创建目的：【根据 菜单ID,获取 所有父节点数据】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @return
     */
    @RequestMapping(value = "/queryResourceById", method = RequestMethod.POST)
    List<Map> queryResourceById(@RequestBody Map paramMap);

    /**
     *
     * @创建人：周礼
     * @创建时间：2015年11月26日
     * @创建目的：【】
     * @修改目的：【修改人：，修改时间：】
     * @param resource
     * @param page
     * @param rows
     * @param sidx
     *@param sord @return
     */
    @RequestMapping(value = "/loadResourceData", method = RequestMethod.POST)
    GridModel<SysResource> loadResourceData(@RequestBody SysResource resource, @RequestParam("page") int page, @RequestParam("rows") int rows,
                                            @RequestParam("sidx") String sidx,@RequestParam("sord") String sord);

    /**
     * 菜单管理
     * @创建人：周礼
     * @创建时间：2015年11月26日
     * @创建目的：【】
     * @修改目的：【修改人：，修改时间：】
     * @param resource
     */
    @RequestMapping(value = "/saveResource", method = RequestMethod.POST)
    void saveResource(@RequestBody SysResource resource);
    /**
     * 菜单管理
     * @创建人：周礼
     * @创建时间：2015年11月27日
     * @创建目的：【】
     * @修改目的：【修改人：，修改时间：】
     * @param id
     * @return
     */
    @RequestMapping(value = "/selectById", method = RequestMethod.POST)
    SysResource selectById(@RequestParam("id") String id);

    /**
     * 菜单管理
     * @创建人：周礼
     * @创建时间：2015年11月27日
     * @创建目的：【】
     * @修改目的：【修改人：，修改时间：】
     * @param resOld
     * @return
     */
    @RequestMapping(value = "/updateByPrimaryKey", method = RequestMethod.POST)
    String updateByPrimaryKey(@RequestBody SysResource resOld);

    /**
     *是否存在子菜单
     * @创建人：周礼
     * @创建时间：2015年11月27日
     * @创建目的：【】
     * @修改目的：【修改人：，修改时间：】
     * @param id
     * @return
     */
    @RequestMapping(value = "/selectChildById", method = RequestMethod.POST)
    boolean selectChildById(@RequestParam("id") String id);

    @RequestMapping(value = "/lockResource", method = RequestMethod.POST)
    void lockResource(@RequestParam("id") String id);

    @RequestMapping(value = "/lockResourceAndChild", method = RequestMethod.POST)
    void lockResourceAndChild(@RequestParam("id") String id);

    @RequestMapping(value = "/selectParentName")
    List<Map<String , Object>> selectParentName();
    /**
     * 菜单管理
     * @创建人：周礼
     * @创建时间：2015年12月4日
     * @创建目的：【通过父菜单id查询父菜单状态】
     * @修改目的：【修改人：，修改时间：】
     * @param parentId
     * @return
     */
    @RequestMapping(value = "/selectParentStatusById",method = RequestMethod.POST)
    boolean selectParentStatusById(@RequestParam("parentId") String parentId);
    /**
     * 菜单管理
     * @创建人：周礼
     * @创建时间：2015年12月4日
     * @创建目的：【激活】
     * @修改目的：【修改人：，修改时间：】
     * @param id
     */
    @RequestMapping(value = "/unlockResource",method = RequestMethod.POST)
    void unlockResource(@RequestParam("id") String id);

    /***
     * 条件查询资源
     * @创建人 XX
     * @创建时间 2016年3月16日
     * @创建目的【】
     * @修改目的【修改人：，修改时间：】
     * @param map
     * @return
     */
    @RequestMapping(value = "/selectResourceList",method = RequestMethod.POST)
    List<SysResource> selectResourceList(@RequestBody Map<String, Object> map);

    /**
     * 加载菜单树数据
     * @param resource Resource
     * @return list
     * @创建人 何睿
     */
    @RequestMapping(value = "/selectTreeViewResource",method = RequestMethod.POST)
    List<TreeViewResource> selectTreeViewResource(@RequestBody SysResource resource);

    @RequestMapping(value = "/deleteByPrimaryKey",method = RequestMethod.POST)
    void deleteByPrimaryKey(@RequestParam("id") String id);

    @RequestMapping(value = "/getResourceByUrl",method = RequestMethod.POST)
	List<SysResource> getResourceByUrl(@RequestParam("url") String url);

    @RequestMapping(value = "/getAllParentResourceListById",method = RequestMethod.POST)
    List<SysResource> getAllParentResourceListById(@RequestParam("id") String id);

    @RequestMapping(value = "/selectbyUserId",method = RequestMethod.POST)
	List<SysResource> selectbyUserId(@RequestParam("sessionUserId") String sessionUserId);

    @RequestMapping(value = "/selectBySysCode",method = RequestMethod.POST)
	List<SysResource> selectBySysCode(@RequestParam("string") String string);

    /**
     * 根据code查询resource,支付模糊匹配
     * @创建人 Ray
     * @创建日期 2016/11/7
     *
     * @param code  type of String
     * @return List<SysResource>
     */
    @RequestMapping(value = "/selectResourceByCode",method = RequestMethod.POST)
	List<SysResource> selectResourceByCode(@RequestParam("code") String code);

    /**
     * 通过父ID查询
     */
    @RequestMapping(value = "/selectResourceByParentId",method = RequestMethod.POST)
    List<SysResource> selectResourceByParentId(@RequestParam("parentId") String parentId);

    @RequestMapping(value = "/deleteById",method = RequestMethod.POST)
    void deleteById(@RequestParam("id") String id);

    @RequestMapping(value = "/deleteByIds",method = RequestMethod.POST)
    void deleteByIds(@RequestBody List<String> ids);

    @RequestMapping(value = "/selectResouceIdByRoleId",method = RequestMethod.POST)
    List<String> selectResouceIdByRoleId(@RequestParam("roleId") String roleId);

}

