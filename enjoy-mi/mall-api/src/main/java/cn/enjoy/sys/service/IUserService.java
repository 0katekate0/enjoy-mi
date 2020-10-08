package cn.enjoy.sys.service;


import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.GridModel;
import cn.enjoy.sys.model.SysUser;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

import java.util.List;
import java.util.Map;

@RequestMapping("/user/sys/service/IUserService")
public interface IUserService {

    /**
     * @param user
     * @param sessionUserId
     * @创建目的：【新增员工】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/addUser", method = RequestMethod.POST)
    void addUser(@RequestBody SysUser user, @RequestParam("sessionUserId") String sessionUserId, @RequestParam("departIds") String departIds, @RequestParam("roleIds") String roleIds);

    /**
     * @param userId
     * @return
     * @创建目的：【通过id查找员工】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/selectByPrimaryKey", method = RequestMethod.POST)
    SysUser selectByPrimaryKey(@RequestParam("userId") String userId);


    /**
     * @param email
     * @return
     * @创建目的：【通过id查找员工】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/selectByEmail", method = RequestMethod.POST)
    SysUser selectByEmail(@RequestParam("email") String email);

    /**
     * @param user
     * @创建目的：【编辑员工】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/editUser", method = RequestMethod.POST)
    void editUser(@RequestBody SysUser user, @RequestParam("sessionUserId") String sessionUserId, @RequestParam("companyId") String companyId);

    /**
     * @param user
     * @创建目的：【冻结用户】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/modifyUserStatusToFreeze", method = RequestMethod.POST)
    void modifyUserStatusToFreeze(@RequestBody SysUser user);

    /**
     * @param user
     * @创建目的：【解冻用户】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/modifyUserStatusToUnFreeze", method = RequestMethod.POST)
    void modifyUserStatusToUnFreeze(@RequestBody SysUser user);

    @RequestMapping(value = "/saveUserRole", method = RequestMethod.POST)
    void saveUserRole(@RequestParam("roleId") String roleId, @RequestBody List<Map<String, String>> map, @RequestParam("crateUserId") String crateUserId, @RequestParam("companyId") String companyId);

    @RequestMapping(value = "/checkUserPassword", method = RequestMethod.POST)
    boolean checkUserPassword(@RequestParam("userId") String userId, @RequestParam("password") String password);

    @RequestMapping(value = "/modifyUserPassWord", method = RequestMethod.POST)
    void modifyUserPassWord(@RequestParam("userId") String userId, @RequestParam("oldPassword") String oldPassword,
                            @RequestParam("newPassword") String newPassword, @RequestParam("repeatPassword") String repeatPassword);

    /***
     * <p><b>description:</b>忘记密码修改</p>
     * @param
     *        userId 用户ID
     *        newPassword    新密码
     *        repeatPassword 确认密码
     */
    @RequestMapping(value = "/modifyUserForgetThePassword", method = RequestMethod.POST)
    void modifyUserForgetThePassword(@RequestParam("userId") String userId, @RequestParam("newPassword") String newPassword, @RequestParam("repeatPassword") String repeatPassword);

    @RequestMapping(value = "/modifyUserMobile", method = RequestMethod.POST)
    void modifyUserMobile(@RequestParam("userId") String userId, @RequestParam("mobile") String mobile);

    @RequestMapping(value = "/modifyUserEmail", method = RequestMethod.POST)
    void modifyUserEmail(@RequestParam("userId") String userId, @RequestParam("email") String email);

    @RequestMapping(value = "/checkUniqueness", method = RequestMethod.POST)
    boolean checkUniqueness(@RequestParam("accountCode") String accountCode);

    @RequestMapping(value = "/modifyUserStatusToCancel", method = RequestMethod.POST)
    void modifyUserStatusToCancel(@RequestBody SysUser user);

    /**
     * @param map
     * @throws BusinessException
     * @创建人：周礼
     * @创建时间：2016年7月12日
     * @创建目的：【密码修改】
     * @修改目的：【修改人：，修改时间：】
     */
    @RequestMapping(value = "/modifyPassword", method = RequestMethod.POST)
    void modifyPassword(@RequestBody Map<String, Object> map) throws BusinessException;

    @RequestMapping(value = "/checkMobileAndEmail", method = RequestMethod.POST)
    boolean checkMobileAndEmail(@RequestParam("email") String email, @RequestParam("userId") String userId, @RequestParam("mobile") String mobile);

    @RequestMapping(value = "/deleteById", method = RequestMethod.POST)
    boolean deleteById(@RequestParam("id") String id);

    @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    void deleteByIds(@RequestBody List<String> ids);

    @RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    GridModel<SysUser> queryByPage(@RequestParam("account") String account, @RequestParam("identityCode") String identityCode, @RequestParam("pageNo") Integer pageNo,
                                   @RequestParam("pageSize") Integer pageSize, @RequestParam("sidx") String sidx,
                                   @RequestParam("sord") String sord);

    @RequestMapping(value = "/queryTimeExtraPage", method = RequestMethod.POST)
    GridModel<Map<String, Object>> queryTimeExtraPage(@RequestParam("account") String account, @RequestParam("pageNo") Integer pageNo,
                                                      @RequestParam("pageSize") Integer pageSize, @RequestParam("sidx") String sidx,
                                                      @RequestParam("sord") String sord);

    @RequestMapping(value = "/queryList", method = RequestMethod.POST)
    List<SysUser> queryList();

    @RequestMapping(value = "/update", method = RequestMethod.POST)
    boolean update(@RequestBody SysUser user, @RequestParam("departIds") String departIds, @RequestParam("roleIds") String roleIds);

    @RequestMapping(value = "/getUserByIdentityCode", method = RequestMethod.POST)
    SysUser getUserByIdentityCode(@RequestParam("identityCode") String identityCode);

    @RequestMapping(value = "/resetPassword", method = RequestMethod.POST)
    void resetPassword(@RequestParam("userId") String userId, @RequestParam("sessionUserId") String sessionUserId);

    @RequestMapping(value = "/updatePassword", method = RequestMethod.POST)
    void updatePassword(@RequestParam("userId") String userId, @RequestParam("originalPwd") String originalPwd,
                        @RequestParam("newPwd") String newPwd, @RequestParam("conPwd") String conPwd, @RequestParam("sessionuserId") String sessionuserId);

    @RequestMapping(value = "/queryContacts", method = RequestMethod.POST)
    GridModel<SysUser> queryContacts(@RequestParam("realName") String realName, @RequestParam("pageNo") Integer pageNo,
                                     @RequestParam("pageSize") Integer pageSize, @RequestParam("sidx") String sidx, @RequestParam("sord") String sord);

    @RequestMapping(value = "/freezeUser", method = RequestMethod.POST)
    void freezeUser(@RequestParam("userId") String userId, @RequestParam("status") String status, @RequestParam("sessionUserId") String sessionUserId);

    @RequestMapping(value = "/updateUserInfo", method = RequestMethod.POST)
    void updateUserInfo(@RequestBody SysUser sysUser);

    @RequestMapping(value = "/register", method = RequestMethod.POST,consumes = MediaType.APPLICATION_JSON_VALUE)
    void register(@RequestBody SysUser user);

    @RequestMapping(value = "/selectByAccount", method = RequestMethod.POST)
    SysUser selectByAccount(@RequestParam("userName") String userName);

}
