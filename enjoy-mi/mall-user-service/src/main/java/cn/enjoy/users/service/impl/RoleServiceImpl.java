
package cn.enjoy.users.service.impl;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.UUIDGenerator;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.model.SysRole;
import cn.enjoy.sys.model.SysRoleResource;
import cn.enjoy.sys.service.IRoleService;
import cn.enjoy.users.dao.SysRoleMapper;
import cn.enjoy.users.dao.SysRoleResourceMapper;
import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 角色管理
 *
 * @author Jack
 * @version 1.0
 */
@RestController
//@RequestMapping("/user/sys/service/IRoleService")
public class RoleServiceImpl implements IRoleService {
    @Autowired
    private SysRoleMapper sysRoleMapper;

    @Autowired
    private SysRoleResourceMapper sysRoleResourceMapper;

    /**
     * 新增角色
     *
     * @param userid
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/addDefaultRoles")
    @Override
    @Transactional
    public void addDefaultRoles(String userid) {
        Map<SysRole, String> defaultMap = this.getDefaultRoles();
        for (Map.Entry<SysRole, String> entry : defaultMap.entrySet()) {
            SysRole role = entry.getKey();
            role.setCreateTime(new Date());
            role.setCreateUser(userid);
            role.setUpdateTime(new Date());
            role.setUpdateUser(userid);
            this.insertRole(entry.getKey(), entry.getValue());
        }
    }

    private Map<SysRole, String> getDefaultRoles() {

        Map<SysRole, String> defaults = new HashMap<SysRole, String>();
        defaults.put(new SysRole("普通员工", "能预订机票、酒店、维护个人资料"), "80,801");
        defaults.put(new SysRole("普通员工", "能预订机票、酒店、维护个人资料"), "802,803");
        return defaults;

    }


    /**
     * 新增角色和业务模块关联
     *
     * @param role
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/addRole")
    @Override
    @Transactional
    public String addRole(SysRole role, String resourceIds) {

        return insertRole(role, resourceIds);
    }

    /**
     * 新增角色
     *
     * @param role
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/addRoleXA")
    @Override
    public String addRoleXA(SysRole role, String resourceIds) {
        return insertRole(role, resourceIds);
    }

    /**
     * 新增角色和角色关联
     *
     * @param role
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    @Override
    public String insertRole(SysRole role, String resourceIds) {
        role.setId(UUIDGenerator.getUUID());
        int count = sysRoleMapper.insertSelective(role);
        //判断角色新增是否成功,失败后不添加业务模块关联,成功才添加业务模块关联
        if (count > 0 && resourceIds != null && !"".equals(resourceIds)) {
            String[] resourceIdStr = resourceIds.split(",");
            if (resourceIdStr.length > 0) {
                //根据数据中moduleId循环添加关联记录
                for (String resourceId : resourceIdStr) {
                    SysRoleResource roleResource = new SysRoleResource();
                    roleResource.setId(UUIDGenerator.getUUID());
                    roleResource.setRoleId(role.getId());
                    roleResource.setResourceId(resourceId);
                    roleResource.setStatus("1");
                    roleResource.setCreateUser(role.getUpdateUser());
                    roleResource.setCreateTime(role.getUpdateTime());
                    sysRoleResourceMapper.insertSelective(roleResource);
                }
            }
        }

        return role.getId();
    }

    /**
     * 修改角色
     *
     * @param role
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/modifyRole")
    @Override
    @Transactional
    public String modifyRole(SysRole role, String resourceIds) {
        return updateRole(role, resourceIds);
    }

    /**
     * 修改角色
     *
     * @param role
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/modifyRoleXA")
    @Override
    public String modifyRoleXA(SysRole role, String resourceIds) {
        return updateRole(role, resourceIds);
    }

    private String updateRole(SysRole role, String resourceIds) {
        int roleCount = sysRoleMapper.updateByPrimaryKeySelective(role);

        //删除业务模块中所有与roleId 相关记录
        int count = sysRoleResourceMapper.deleteByRoleId(role.getId());
        if (roleCount > 0 && resourceIds != null && !"".equals(resourceIds)) {
            String[] resourceIdStr = resourceIds.split(",");
            if (resourceIdStr.length > 0) {
                //根据数据中moduleId循环添加关联记录
                for (String resourceId : resourceIdStr) {
                    SysRoleResource roleResource = new SysRoleResource();
                    roleResource.setId(UUIDGenerator.getUUID());
                    roleResource.setRoleId(role.getId());
                    roleResource.setResourceId(resourceId);
                    roleResource.setCreateUser(role.getUpdateUser());
                    roleResource.setCreateTime(role.getUpdateTime());
                    sysRoleResourceMapper.insertSelective(roleResource);
                }
            }
        }

        return role.getId();
    }

    /**
     * 删除角色
     *
     * @param roleId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/deleteRoleById")
    @Override
    @Transactional
    public String deleteRoleById(String roleId) {
        //判断角色删除是否成功
        int count = sysRoleResourceMapper.deleteByRoleId(roleId);
        int roleCount = sysRoleMapper.deleteByPrimaryKey(roleId);

        return roleId;
    }

    /**
     * 查询角色详情
     *
     * @param roleId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/queryRoleDetailById")
    @Override
    public SysRole queryRoleDetailById(String roleId) {
        return sysRoleMapper.selectByPrimaryKey(roleId);
    }

    /**
     * 根据菜单id查询菜单
     *
     * @param userId
     * @param resourceId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/findRoleByResourceId")
    @Override
    public List<SysRole> findRoleByResourceId(String userId, String resourceId) {
        Map<String, Object> paramMap = new HashMap<String, Object>();
        paramMap.put("userId", userId);
        paramMap.put("resourceId", resourceId);
        return sysRoleMapper.queryRoleByResourceId(paramMap);
    }

    /**
     * 新增角色和角色菜单关联
     *
     * @param initRole
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/saveInitRole")
    @Override
    @Transactional
    public String saveInitRole(SysRole initRole, String resourceIds) {

        initRole.setId(UUIDGenerator.getUUID());
        int count = sysRoleMapper.insertSelective(initRole);
        //判断角色新增是否成功,失败后不添加业务模块关联,成功才添加业务模块关联
        if (count > 0 && resourceIds != null && !"".equals(resourceIds)) {
            String[] resourceIdStr = resourceIds.split(",");
            if (resourceIdStr.length > 0) {
                //根据数据中moduleId循环添加关联记录
                for (String resourceId : resourceIdStr) {
                    SysRoleResource initRoleResource = new SysRoleResource();
                    initRoleResource.setId(UUIDGenerator.getUUID());
                    initRoleResource.setRoleId(initRole.getId());
                    initRoleResource.setResourceId(resourceId);
                    initRoleResource.setCreateUser(initRole.getCreateUser());
                    initRoleResource.setCreateTime(initRole.getCreateTime());
                    sysRoleResourceMapper.insertSelective(initRoleResource);
                }
            }
        }

        return initRole.getId();
    }

    /**
     * 修改角色和角色菜单关联
     *
     * @param initRole
     * @param resourceIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/modifyInitRole")
    @Override
    public String modifyInitRole(SysRole initRole, String resourceIds) {

        int roleCount = sysRoleMapper.updateByPrimaryKeySelective(initRole);

        //删除业务模块中所有与roleId 相关记录
        int count = sysRoleResourceMapper.deleteByRoleId(initRole.getId());
        if (roleCount > 0 && resourceIds != null && !"".equals(resourceIds)) {
            String[] resourceIdStr = resourceIds.split(",");
            if (resourceIdStr.length > 0) {
                //根据数据中moduleId循环添加关联记录
                for (String resourceId : resourceIdStr) {
                    SysRoleResource initRoleResource = new SysRoleResource();
                    initRoleResource.setId(UUIDGenerator.getUUID());
                    initRoleResource.setRoleId(initRole.getId());
                    initRoleResource.setResourceId(resourceId);
                    initRoleResource.setCreateUser(initRole.getCreateUser());
                    initRoleResource.setCreateTime(initRole.getCreateTime());
                    sysRoleResourceMapper.insertSelective(initRoleResource);
                }
            }
        }

        return initRole.getId();
    }

    /**
     * 删除角色和角色菜单关联
     *
     * @param roleId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/deleteInitRoleById")
    @Override
    @Transactional
    public String deleteInitRoleById(String roleId) {
        //判断角色删除是否成功
        int roleCount = sysRoleResourceMapper.deleteByRoleId(roleId);
        int count = sysRoleMapper.deleteByPrimaryKey(roleId);

        return roleId;
    }

    /**
     * 删除角色和角色菜单关联
     *
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/deleteByIds")
    @Override
    @Transactional
    public void deleteByIds(List<String> ids) {
        if (!CollectionUtils.isEmpty(ids)) {
            for (String roleId : ids) {
                sysRoleResourceMapper.deleteByRoleId(roleId);
                sysRoleMapper.deleteByPrimaryKey(roleId);
            }
        }
    }

    /**
     * 查询全量角色数据
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/queryRoleList")
    @Override
    public List<SysRole> queryRoleList() {
        return sysRoleMapper.queryRoleList();
    }


    @Override
    public SysRole selectRoleById(SysRole role) {
        return null;
    }

    /**
     * 根据用户id查询用户角色
     *
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/queryUserRoleByUserId")
    @Override
    public List<SysRole> queryUserRoleByUserId(String userId) {
        return sysRoleMapper.queryUserRoleByUserId(userId);
    }

    /**
     * 根据角色编码查询角色
     *
     * @param roleCode
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/selectByRoleCode")
    @Override
    public SysRole selectByRoleCode(String roleCode) {
        return sysRoleMapper.selectByRoleCode(roleCode);
    }

    /**
     * 根据主键查询角色
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/selectRoleById")
    @Override
    public SysRole selectRoleById(String id) {
        return sysRoleMapper.selectByPrimaryKey(id);
    }

    /**
     * 查询默认角色
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/selectDefaultRole")
    @Override
    public List<SelectModel> selectDefaultRole() {
        return sysRoleMapper.selectDefaultRole();
    }

    /**
     * 查询角色列表
     *
     * @param param
     * @param pageNum
     * @param pageSize
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@PostMapping("/selectRolePage")
    @Override
    public GridModel<SysRole> selectRolePage(String param, int pageNum, int pageSize, String sidx, String sord) {
        String orderString = "update_time.desc";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        } else {
            orderString = "update_time.desc";
        }
        PageBounds pageBounds = new PageBounds(pageNum, pageSize, Order.formString(orderString));
        PageList<SysRole> rolePage = (PageList<SysRole>) sysRoleMapper.selectByPage(param, pageBounds);
        return new GridModel<SysRole>(rolePage);
    }
}

