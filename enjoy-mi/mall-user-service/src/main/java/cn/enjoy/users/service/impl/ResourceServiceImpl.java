package cn.enjoy.users.service.impl;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.UUIDGenerator;
import cn.enjoy.sys.model.ResourceTree;
import cn.enjoy.sys.model.SysResource;
import cn.enjoy.sys.model.SysRoleResource;
import cn.enjoy.sys.model.TreeViewResource;
import cn.enjoy.sys.service.IResourceService;
import cn.enjoy.users.dao.SysResourceMapper;
import cn.enjoy.users.dao.SysRoleResourceMapper;
import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 菜单管理
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/user/sys/service/IResourceService")
public class ResourceServiceImpl implements IResourceService {
    @Resource
    private SysResourceMapper sysResourceMapper;

    @Resource
    private SysRoleResourceMapper sysRoleResourceMapper;

    @Override
    public String getMenuIdByUrl(String url) {
        return sysResourceMapper.getIdByUrl(url);
    }


    /**
     * 查询菜单树
     *
     * @param roleId
     * @param parentId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryResourceTree", method = RequestMethod.POST)
    @Override
    public List<ResourceTree> queryResourceTree(String roleId, String parentId) {
        List<ResourceTree> resources;
        if (roleId != null && !"".equals(roleId)) {
            resources = sysResourceMapper.selectModifyResourceTree(roleId, parentId);
        } else {
            resources = sysResourceMapper.selectAddResourceTree(parentId);
        }
        return resources;
    }


    /**
     * 查询初始化的菜单树
     *
     * @param roleId
     * @param parentId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryInitResourceTree", method = RequestMethod.POST)
    @Override
    public List<ResourceTree> queryInitResourceTree(String roleId, String parentId) {
        List<ResourceTree> resources = null;
        if (roleId != null && !"".equals(roleId)) {
//            resources = sysResourceMapper.selectInitModifyResourceTree(roleId,parentId,sysCode,comType);
        } else {
            resources = sysResourceMapper.selectAddResourceTree(parentId);
        }
        return resources;
    }

    /**
     * 根据id查询菜单数据
     *
     * @param paramMap
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryResourceById", method = RequestMethod.POST)
    @Override
    public List<Map> queryResourceById(Map paramMap) {
        return this.sysResourceMapper.queryResourceById(paramMap);
    }

    /**
     * 查询菜单列表
     *
     * @param resource
     * @param page
     * @param rows
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/loadResourceData", method = RequestMethod.POST)
    @Override
    public GridModel<SysResource> loadResourceData(SysResource resource, int page, int rows, String sidx, String sord) {
        String orderString = "";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        } else {
            orderString = "status.desc,sort_no";
        }
        PageBounds pageBounds = new PageBounds(page, rows, Order.formString(orderString));
        PageList<SysResource> pageList = sysResourceMapper.queryResourcePage(resource, pageBounds);
        return new GridModel<SysResource>(pageList);
    }

    /**
     * 保存菜单
     *
     * @param resource
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/saveResource", method = RequestMethod.POST)
    @Override
    public void saveResource(SysResource resource) {
        resource.setId(UUIDGenerator.getUUID());
        //新增资源
        if ("".equals(resource.getParentId())) {
            resource.setParentId(null);
        }
        sysResourceMapper.insertSelective(resource);

    }

    /**
     * 根据id查询菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectById", method = RequestMethod.POST)
    @Override
    public SysResource selectById(String id) {
        return sysResourceMapper.selectByPrimaryKey(id);
    }

    /**
     * 更新菜单
     *
     * @param resOld
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateByPrimaryKey", method = RequestMethod.POST)
    @Override
    public String updateByPrimaryKey(SysResource resOld) {
        sysResourceMapper.updateByPrimaryKeySelective(resOld);

        return null;
    }

    /**
     * 根据id查询子菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectChildById", method = RequestMethod.POST)
    @Override
    public boolean selectChildById(String id) {
        if (sysResourceMapper.selectChildById(id) > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 锁住菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/lockResource", method = RequestMethod.POST)
    @Override
    public void lockResource(String id) {
        sysResourceMapper.lockByPrimaryKey(id);
    }

    /**
     * 锁住菜单和子菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/lockResourceAndChild", method = RequestMethod.POST)
    @Override
    public void lockResourceAndChild(String id) {
        sysResourceMapper.lockResourceAndChild(id);
        sysResourceMapper.lockByPrimaryKey(id);
    }

    /**
     * 下拉框式菜单数据
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectParentName")
    @Override
    public List<Map<String, Object>> selectParentName() {
        return sysResourceMapper.selectParentName();
    }

    /**
     * 查询父的状态
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectParentStatusById", method = RequestMethod.POST)
    @Override
    public boolean selectParentStatusById(String id) {
        int status = sysResourceMapper.selectParentStatusById(id);
        return status == 1;
    }

    /**
     * 解锁菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/unlockResource", method = RequestMethod.POST)
    @Override
    public void unlockResource(String id) {
        sysResourceMapper.unlockResource(id);
    }


    /**
     * 根据条件查询菜单
     *
     * @param map
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectResourceList", method = RequestMethod.POST)
    @Override
    public List<SysResource> selectResourceList(Map<String, Object> map) {
        return sysResourceMapper.selectResourceList(map);
    }

    /**
     * 查询菜单树
     *
     * @param resource
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectTreeViewResource", method = RequestMethod.POST)
    @Override
    public List<TreeViewResource> selectTreeViewResource(SysResource resource) {
        return sysResourceMapper.selectTreeViewResource(resource);
    }

    /**
     * 删除菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/deleteByPrimaryKey", method = RequestMethod.POST)
    @Override
    public void deleteByPrimaryKey(String id) {
        sysResourceMapper.deleteByPrimaryKey(id);
    }

    /**
     * 根据url查询菜单
     *
     * @param url
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/getResourceByUrl", method = RequestMethod.POST)
    @Override
    public List<SysResource> getResourceByUrl(String url) {
        return sysResourceMapper.queryResourceByUrl(url);
    }

    /**
     * 根据id查询所有的父菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/getAllParentResourceListById", method = RequestMethod.POST)
    @Override
    public List<SysResource> getAllParentResourceListById(String id) {
        List<SysResource> allResourceList = sysResourceMapper.selectResourceList(new HashMap<String, Object>());
        List<SysResource> result_list = new ArrayList<SysResource>();
        for (SysResource r : allResourceList) {
            if (r.getId().equals(id)) {
                result_list.add(r);
                if (!StringUtils.isEmpty(r.getParentId())) {
                    getResourceListByParentId(r, result_list, allResourceList);
                }
            }
        }
        return result_list;
    }

    private void getResourceListByParentId(SysResource resource, List<SysResource> result_list, List<SysResource> allResourceList) {
        for (SysResource r : allResourceList) {
            if (r.getId().equals(resource.getParentId())) {
                result_list.add(r);
                if (!StringUtils.isEmpty(r.getParentId())) {
                    getResourceListByParentId(r, result_list, allResourceList);
                }
            }
        }
    }


    /**
     * 根据用户查询菜单
     *
     * @param sessionUserId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectbyUserId", method = RequestMethod.POST)
    @Override
    public List<SysResource> selectbyUserId(String sessionUserId) {

        return sysResourceMapper.selectbyUserId(sessionUserId);
    }

    /**
     * 根据编码查询菜单
     *
     * @param string
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectBySysCode", method = RequestMethod.POST)
    @Override
    public List<SysResource> selectBySysCode(String string) {

        return sysResourceMapper.sectbySysCode(string);
    }

    /**
     * 根据编码查询菜单
     *
     * @param code
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectResourceByCode", method = RequestMethod.POST)
    @Override
    public List<SysResource> selectResourceByCode(String code) {
        return sysResourceMapper.selectResourceByCode(code);
    }

    /**
     * 根据父id查询菜单
     *
     * @param parentId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectResourceByParentId", method = RequestMethod.POST)
    @Override
    public List<SysResource> selectResourceByParentId(String parentId) {
        Map<String, Object> paramMap = new HashMap();
        paramMap.put("parentId", parentId);
        return sysResourceMapper.selectbyParentId(parentId);
    }

    /**
     * 根据id删除菜单
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/deleteById", method = RequestMethod.POST)
    @Override
    @Transactional
    public void deleteById(String id) {
        sysRoleResourceMapper.deleteByResourceId(id);
        sysResourceMapper.deleteByPrimaryKey(id);
    }

    /**
     * 根据ids删除菜单
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
    @Transactional
    public void deleteByIds(List<String> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            for (String id : ids) {
                sysRoleResourceMapper.deleteByResourceId(id);
                sysResourceMapper.deleteByPrimaryKey(id);
            }
        }
    }

    /**
     * 根据角色id查询菜单
     *
     * @param roleId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectResouceIdByRoleId", method = RequestMethod.POST)
    @Override
    public List<String> selectResouceIdByRoleId(String roleId) {
        List<SysRoleResource> sysRoleResources = sysRoleResourceMapper.selectResouceByRoleId(roleId);
        if (!CollectionUtils.isEmpty(sysRoleResources)) {
            return sysRoleResources.stream().map(res -> res.getResourceId()).collect(Collectors.toList());
        }
        return null;
    }
}

