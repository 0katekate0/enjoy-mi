/**
 * Created on 2015年9月9日 by Caiming
 */
package cn.enjoy.sys.service;


import cn.enjoy.core.utils.GridModel;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.model.SysRole;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;

@RequestMapping("/user/sys/service/IRoleService")
public interface IRoleService {

    /***
     * 根据角色ID 查询角色和模块组
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月10日</li>
    List<SelectModel> findRolesSelectModel(String sessionUserCompanyId, int parseInt);

//    List<SelectModel> findRolesSelectModel(String companyId,Integer loginType);
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param role
     * @return
     */
    @PostMapping("/selectRole")
    public SysRole selectRoleById(@RequestBody SysRole role);

    /***
     * 新增角色和业务模块关联
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月10日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param role
     * @param resourceIds
     * @return
     */
    @PostMapping("/addRole")
    public String addRole(@RequestBody SysRole role, @RequestParam("resourceIds") String resourceIds);

    @PostMapping("/addRoleXA")
    public String addRoleXA(@RequestBody SysRole role, @RequestParam("resourceIds") String resourceIds);

    /**
     * 编辑角色和角色业务模块关联
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月10日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param role
     * @param resourceId
     * @return
     */
    @PostMapping("/modifyRole")
    public String modifyRole(@RequestBody SysRole role, @RequestParam("resourceId") String resourceId);

    @PostMapping("/modifyRoleXA")
    public String modifyRoleXA(@RequestBody SysRole role, @RequestParam("resourceId") String resourceId);

    /***
     * 删除角色并删除角色业务模块关联
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月10日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param roleId
     * @return
     */
    @PostMapping("/deleteRoleById")
    public String deleteRoleById(@RequestParam("roleId") String roleId);
    /***
     * 根据ID查询角色明细
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月29日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param roleId
     * @return
     */
    @PostMapping("/queryRoleDetailById")
    SysRole queryRoleDetailById(@RequestParam("roleId") String roleId);


    /**
     * 根据用户ID和资源ID查询角色
     * <li>创建人：Caiming</li>
     * <li>创建时间：2015年10月20日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param userId
     * @param resourceId
     * @return
     */
    @PostMapping("/findRoleByResourceId")
    List<SysRole> findRoleByResourceId(@RequestParam("userId") String userId, @RequestParam("resourceId") String resourceId);


    /***
     * 新增初始化角色和业务模块关联
     * <li>创建人：xiaopu</li>
     * <li>创建时间：2015年9月10日</li>
     * <li>创建目的：【】</li>
     * <li>修改目的：【修改人：，修改时间：】</li>
     * @param initRole
     * @param resourceIds
     * @return String
     */
    @PostMapping("/saveInitRole")
    public String saveInitRole(@RequestBody SysRole initRole, @RequestParam("resourceIds") String resourceIds);

    @PostMapping("/modifyInitRole")
    public String modifyInitRole(@RequestBody SysRole initRole, @RequestParam("resourceIds") String resourceIds);

    @PostMapping("/deleteInitRoleById")
    public String deleteInitRoleById(@RequestParam("roleId") String roleId);

    @PostMapping("/queryUserRoleByUserId")
    List<SysRole> queryUserRoleByUserId(@RequestParam("userId") String userId);

    @PostMapping("/selectByRoleCode")
    SysRole selectByRoleCode(@RequestParam("roleCode") String roleCode);

    @PostMapping("/addDefaultRoles")
	void addDefaultRoles(@RequestParam("userid") String userid);

    @PostMapping("/insertRole")
	String insertRole(@RequestBody SysRole role, @RequestParam("resourceIds") String resourceIds);

    @PostMapping("/selectRoleById")
	SysRole selectRoleById(@RequestParam("id") String id);

    @PostMapping("/selectDefaultRole")
	List<SelectModel> selectDefaultRole();

    @PostMapping("/selectRolePage")
    public GridModel<SysRole> selectRolePage(@RequestParam("param") String param, @RequestParam("pageNum") int pageNum, @RequestParam("pageSize") int pageSize, @RequestParam("sidx") String sidx, @RequestParam("sord") String sord);

    @PostMapping("/deleteByIds")
    void deleteByIds(@RequestBody List<String> ids);

    @PostMapping("/queryRoleList")
    List<SysRole> queryRoleList();
}

