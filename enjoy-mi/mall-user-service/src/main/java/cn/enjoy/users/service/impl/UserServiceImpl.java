/**
 * Created on 2016年6月15日 by 周礼
 */
package cn.enjoy.users.service.impl;


import cn.enjoy.core.constant.Constants;
import cn.enjoy.core.constant.SysRoleEnum;
import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.*;
import cn.enjoy.core.utils.response.ResponseCodeConstant;
import cn.enjoy.sys.model.SysUser;
import cn.enjoy.sys.model.SysUserRole;
import cn.enjoy.sys.model.UserDepartmentInfo;
import cn.enjoy.sys.service.IUserService;
import cn.enjoy.users.dao.SysUserMapper;
import cn.enjoy.users.dao.SysUserRoleMapper;
import cn.enjoy.users.dao.UserDepartmentInfoMapper;
import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.pagehelper.PageHelper;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.*;

/**
 * 用户管理
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/user/sys/service/IUserService")
public class UserServiceImpl implements IUserService {
    @Resource
    private SysUserMapper sysUserMapper;
    @Resource
    private SysUserRoleMapper sysUserRoleMapper;
    @Resource
    private UserDepartmentInfoMapper userDepartmentInfoMapper;


    /**
     * 新增用户
     *
     * @param record
     * @param sessionUserId
     * @param departIds
     * @param roleIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/addUser", method = RequestMethod.POST)
    @Override
    @Transactional
    public void addUser(SysUser record, String sessionUserId, String departIds, String roleIds) {
        if (StringUtils.isEmpty(record.getId())) {
            //新增用户校验邮箱是否已被注册
            int count = sysUserMapper.checkMobileAndEmail(CommonConstant.USER_STATUS_YES_ACTIVATION, record.getEmail(), null, null, null);
            int count2 = sysUserMapper.checkMobileAndEmail(CommonConstant.USER_STATUS_YES_ACTIVATION, null, record.getMobile(), null, null);
            if (count > 0) {
                throw new BusinessException("此邮箱地址已经被注册");
            }
            if (count2 > 0) {
                throw new BusinessException("此手机号码已经被注册");
            }
            record.setId(UUIDGenerator.getUUID());

        }
        record.setStatus(CommonConstant.USER_STATUS_YES_ACTIVATION);//置为已激活
        //record.setIdentityCode(CommonConstant.ACCOUNT_TYPE_COMMON_USER);
        record.setSysCode(Constants.BACKGROUNDCODE);
        String code = RandomUtils.emailActiveValiDateCode(record.getEmail());
        record.setValidateCode(code);
        record.setActivatedTime(new Date());
        record.setCreateTime(new Date());
        //record.setDepartId(departId);
        record.setUpdateTime(new Date());
        String password = record.getPassword();//初始密码
        String passwordRand = CommonConstant.INIT_PASSWORD_RAND;//处理秘钥因子
        String md5Password = MD5Util.generateMD5(password, passwordRand);
        record.setPassword(md5Password);
        record.setPasswordRand(passwordRand);
        record.setRoleIds(Arrays.asList(roleIds));
        List<SysUserRole> roleList = new ArrayList<SysUserRole>();
        if (record.getRoleIds() != null && record.getRoleIds().size() != 0) {
            for (String roleId : record.getRoleIds()) {
                SysUserRole userRole = new SysUserRole();
                userRole.setId(UUIDGenerator.getUUID());
                userRole.setRoleId(roleId);
                userRole.setUserId(record.getId());
                userRole.setCreateTime(new Date());
                userRole.setCreateUser(sessionUserId);
                roleList.add(userRole);
            }
        }
        sysUserMapper.insertSelective(record);
        if (roleList.size() != 0) {
            sysUserRoleMapper.insertUserRoleByRoleId(roleList);
        }

        saveUserDeparts(record.getId(), sessionUserId, Arrays.asList(departIds));

        //this.saveEmployee((UserVo) record);
    }

    public void saveUserRole(SysUser record, String sessionUserId, List<String> roleIds) {
        List<SysUserRole> roleList = new ArrayList<SysUserRole>();
        if (record.getRoleIds() != null && roleIds.size() != 0) {
            for (String roleId : roleIds) {
                SysUserRole userRole = new SysUserRole();
                userRole.setId(UUIDGenerator.getUUID());
                userRole.setRoleId(roleId);
                userRole.setUserId(record.getId());
                userRole.setCreateTime(new Date());
                userRole.setCreateUser(sessionUserId);
                roleList.add(userRole);
            }
            if (roleList.size() != 0) {
                sysUserRoleMapper.insertUserRoleByRoleId(roleList);
            }
        }
    }

    private void saveUserDeparts(String UserId, String sessionUserId, List<String> departIds) {
        List<UserDepartmentInfo> udis = new ArrayList<>();
        if (!CollectionUtils.isEmpty(departIds)) {
            for (String dpartId : departIds) {
                UserDepartmentInfo udi = new UserDepartmentInfo();
                udi.setCreateTime(new Date());
                udi.setCreateUser(sessionUserId);
                udi.setId(UUIDGenerator.getUUID());
                udi.setUserId(UserId);
                udi.setDepartmentId(dpartId);
                udis.add(udi);
            }
            userDepartmentInfoMapper.insertUserDepartInfo(udis);
        }
    }

    /**
     * 查询用户
     *
     * @param userId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByPrimaryKey", method = RequestMethod.POST)
    @Override
    public SysUser selectByPrimaryKey(String userId) {
        return sysUserMapper.selectByPrimaryKey(userId);
    }


    /**
     * 根据email查询用户
     *
     * @param email
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByEmail", method = RequestMethod.POST)
    @Override
    public SysUser selectByEmail(String email) {
        return sysUserMapper.selectByEmail(email);
    }


    /**
     * 保存用户角色
     *
     * @param roleId
     * @param map
     * @param crateUserId
     * @param departId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/saveUserRole", method = RequestMethod.POST)
    @Override
    @Transactional
    public void saveUserRole(String roleId, List<Map<String, String>> map, String crateUserId, String departId) {
        Map<String, Object> dm = new HashMap<String, Object>();
        dm.put("roleId", roleId);
        dm.put("departId", departId);
        sysUserRoleMapper.deleteUserRoleByUserIds(dm);
        List<SysUserRole> userRoles = new ArrayList<SysUserRole>();
        if (map != null && map.size() != 0) {
            for (int i = 0; i < map.size(); i++) {
                SysUserRole userRole = new SysUserRole();
                Map<String, String> m = map.get(i);
                userRole.setId(UUIDGenerator.getUUID());
                userRole.setRoleId(roleId);
                userRole.setUserId(m.get("userId"));
                userRole.setCreateTime(new Date());
                userRole.setCreateUser(crateUserId);
                userRoles.add(userRole);
            }
            sysUserRoleMapper.insertUserRoleByRoleId(userRoles);
        }

    }

    /**
     * 校验用户名和密码
     *
     * @param userId
     * @param password
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/checkUserPassword", method = RequestMethod.POST)
    @Override
    public boolean checkUserPassword(String userId, String password) {
        SysUser user = sysUserMapper.selectByPrimaryKey(userId);
        if (user == null) {
            throw new BusinessException("用户不存在");
        }
        String md5Password = MD5Util.generateMD5(password,
                user.getPasswordRand());
        if (!user.getPassword().equals(md5Password)) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 修改密码
     *
     * @param userId
     * @param oldPassword
     * @param newPassword
     * @param repeatPassword
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserPassWord", method = RequestMethod.POST)
    @Override
    public void modifyUserPassWord(String userId, String oldPassword,
                                   String newPassword, String repeatPassword) {
        if (!newPassword.equals(repeatPassword)) {
            throw new BusinessException(
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL,
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL_UNLIKE_MSG);
        }
        if (!checkUserPassword(userId, oldPassword)) {
            throw new BusinessException(
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL,
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL_NOTPASS_MSG);
        }
        String passwordRandom = RandomUtils.generateNumberString(8);
        String md5Password = MD5Util.generateMD5(newPassword, passwordRandom);
        // 临时对象,避免再写单独的密码更新语句
        SysUser tempUser = new SysUser();
        tempUser.setId(userId);
        tempUser.setPassword(md5Password);
        tempUser.setPasswordRand(passwordRandom);
        sysUserMapper.updateByPrimaryKeySelective(tempUser);
    }

    /**
     * 忘记密码修改
     *
     * @param userId
     * @param newPassword
     * @param repeatPassword
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserForgetThePassword", method = RequestMethod.POST)
    @Override
    public void modifyUserForgetThePassword(String userId, String newPassword, String repeatPassword) {
        if (!newPassword.equals(repeatPassword)) {
            throw new BusinessException(
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL,
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL_UNLIKE_MSG);
        }
        String passwordRandom = RandomUtils.generateNumberString(8);
        String md5Password = MD5Util.generateMD5(newPassword, passwordRandom);
        // 临时对象,避免再写单独的密码更新语句
        SysUser tempUser = new SysUser();
        tempUser.setId(userId);
        tempUser.setPassword(md5Password);
        tempUser.setPasswordRand(passwordRandom);
        sysUserMapper.updateByPrimaryKeySelective(tempUser);
    }

    /**
     * 修改用户手机号
     *
     * @param userId
     * @param mobile
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserMobile", method = RequestMethod.POST)
    @Override
    public void modifyUserMobile(String userId, String mobile) {
        SysUser user = sysUserMapper.selectByPrimaryKey(userId);
        if (user == null) {
            throw new BusinessException("用户不存在");
        }
        user.setMobile(mobile);
        sysUserMapper.updateByPrimaryKeySelective(user);
    }

    /**
     * 修改用户邮箱
     *
     * @param userId
     * @param email
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserEmail", method = RequestMethod.POST)
    @Override
    public void modifyUserEmail(String userId, String email) {
        SysUser user = sysUserMapper.selectByPrimaryKey(userId);
        if (user == null) {
            throw new BusinessException("用户不存在");
        }
        user.setEmail(email);
        sysUserMapper.updateByPrimaryKeySelective(user);
    }

    /**
     * 校验用户是否唯一
     *
     * @param accountCode
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/checkUniqueness", method = RequestMethod.POST)
    @Override
    public boolean checkUniqueness(String accountCode) throws BusinessException {
        // 传入的查询条件非空校验
        if (StringUtils.isEmpty(accountCode)) {
            return true;
        }
        // 已作废的用户不在检验范围之内
        if (sysUserMapper.checkUniqueness(
                String.valueOf(CommonConstant.USER_STATUS_CANCEL), accountCode) > 1) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * 修改用户信息
     *
     * @param user
     * @param sessionUserId
     * @param companyId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/editUser", method = RequestMethod.POST)
    @Override
    @Transactional
    public void editUser(SysUser user, String sessionUserId, String companyId) {
        if (!StringUtils.isEmpty(user.getId())) {
            if (user.getRoleIds() != null && user.getRoleIds().size() != 0) {
                sysUserRoleMapper.deleteUserRoleByUserId(user.getId());
                List<SysUserRole> roleList = new ArrayList<SysUserRole>();
                for (String roleId : user.getRoleIds()) {
                    SysUserRole userRole = new SysUserRole();
                    userRole.setId(UUIDGenerator.getUUID());
                    userRole.setRoleId(roleId);
                    userRole.setUserId(user.getId());
                    userRole.setCreateTime(new Date());
                    userRole.setCreateUser(sessionUserId);
                    roleList.add(userRole);
                }
                sysUserRoleMapper.insertUserRoleByRoleId(roleList);
            } else {
                String password = user.getPassword();//初始密码
                String passwordRand = CommonConstant.INIT_PASSWORD_RAND;//处理秘钥因子
                String md5Password = MD5Util.generateMD5(password, passwordRand);
                user.setPassword(md5Password);
                user.setPasswordRand(passwordRand);
            }
            sysUserMapper.updateByPrimaryKeySelective(user);
        }
    }

    /**
     * 冻结用户
     *
     * @param user
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserStatusToFreeze", method = RequestMethod.POST)
    @Override
    @Transactional
    public void modifyUserStatusToFreeze(SysUser user) {
        user.setStatus(CommonConstant.USER_STATUS_FREEZE);
        sysUserMapper.updateByPrimaryKeySelective(user);

    }

    /**
     * 解冻用户
     *
     * @param user
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserStatusToUnFreeze", method = RequestMethod.POST)
    @Override
    @Transactional
    public void modifyUserStatusToUnFreeze(SysUser user) {
        user.setStatus(CommonConstant.USER_STATUS_YES_ACTIVATION);
        sysUserMapper.updateByPrimaryKeySelective(user);
    }

    /**
     * 关闭用户
     *
     * @param user
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyUserStatusToCancel", method = RequestMethod.POST)
    @Transactional
    @Override
    public void modifyUserStatusToCancel(SysUser user) {
        user.setStatus(CommonConstant.USER_STATUS_CANCEL);
        sysUserMapper.updateByPrimaryKeySelective(user);
    }


    /**
     * 修改密码
     *
     * @param map
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/modifyPassword", method = RequestMethod.POST)
    @Override
    public void modifyPassword(Map<String, Object> map)
            throws BusinessException {
        if (!(map.get("newPassword").toString()).equals(map.get("rePassword").toString())) {
            throw new BusinessException(ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL,
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL_UNLIKE_MSG);
        }
        if (!checkUserPassword(map)) {
            throw new BusinessException(ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL,
                    ResponseCodeConstant.USER_PASSWORD_MODIFY_FAIL_NOTPASS_MSG);
        }
        SysUser user = sysUserMapper.selectByPrimaryKey(map.get("userId").toString());
        if (user != null) {
            String md5Password = MD5Util.generateMD5(map.get("newPassword").toString(), user.getPasswordRand());
            SysUser tempUser = new SysUser();
            tempUser.setId(user.getId());
            tempUser.setPassword(md5Password);
            sysUserMapper.updateByPrimaryKeySelective(tempUser);
        }

    }

    public boolean checkUserPassword(Map<String, Object> map) {
        SysUser user = sysUserMapper.selectByPrimaryKey(map.get("userId").toString());
        if (user == null) {
            throw new BusinessException(ResponseCodeConstant.USER_LOGIN_FAIL, "用户不存在");
        }
        String md5Password = MD5Util.generateMD5(map.get("oldPassword").toString(), user.getPasswordRand());
        if (!user.getPassword().equals(md5Password)) {
            return false;
        } else {
            return true;
        }

    }

    /**
     * 校验手机号和邮箱是否存在
     *
     * @param email
     * @param userId
     * @param mobile
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/checkMobileAndEmail", method = RequestMethod.POST)
    @Override
    public boolean checkMobileAndEmail(String email, String userId, String mobile) {
        int count = sysUserMapper.checkMobileAndEmail(CommonConstant.USER_STATUS_YES_ACTIVATION, email, mobile, Constants.BACKGROUNDCODE, userId);
        if (count > 0) {
            return false;
        }
        return true;
    }

    /**
     * 删除用户
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
    public boolean deleteById(String id) {
        int count = sysUserMapper.deleteByPrimaryKey(id);
        if (count > 0) {
            return false;
        }
        return true;
    }

    /**
     * 批量删除用户
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
    public void deleteByIds(List<String> ids) {
        sysUserMapper.batchDeleteUsers(ids);
    }

    /**
     * 查询用户列表
     *
     * @param account
     * @param identityCode
     * @param pageNo
     * @param pageSize
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<SysUser> queryByPage(String account, String identityCode, Integer pageNo, Integer pageSize, String sidx, String sord) {
        String orderString = "";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        }
        PageHelper.startPage(pageNo, pageSize);
//        PageBounds pageBounds = new PageBounds(pageNo, pageSize, Order.formString(orderString));
        List<SysUser> pageList = sysUserMapper.queryByPage(account/*, Integer.valueOf(identityCode)*/);
        return new GridModel<SysUser>(pageList);
    }

    /**
     * 工时补录查询
     *
     * @param account
     * @param pageNo
     * @param pageSize
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryTimeExtraPage", method = RequestMethod.POST)
    @Override
    public GridModel<Map<String, Object>> queryTimeExtraPage(String account, Integer pageNo, Integer pageSize, String sidx, String sord) {
        String orderString = "";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        }
        PageBounds pageBounds = new PageBounds(pageNo, pageSize, Order.formString(orderString));
        PageList<Map<String, Object>> pageList = (PageList<Map<String, Object>>) sysUserMapper.queryTimeExtraPage(account, pageBounds);
        return new GridModel<>(pageList);
    }

    /**
     * 查询全量用户
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryList", method = RequestMethod.POST)
    @Override
    public List<SysUser> queryList() {
        return sysUserMapper.queryList();
    }

    /**
     * 更新用户信息
     *
     * @param user
     * @param departIds
     * @param roleIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/update", method = RequestMethod.POST)
    @Override
    @Transactional
    public boolean update(SysUser user, String departIds, String roleIds) {
        boolean a = true;
        List<SysUser> userList = sysUserMapper.selectAllEmail(user.getId());
        for (SysUser sysUser : userList) {
            if (user.getEmail().equals(sysUser.getEmail())) {
                a = false;
            }
        }
        int result = sysUserMapper.checkMobileAndEmail(CommonConstant.USER_STATUS_YES_ACTIVATION, null, user.getMobile(), null, user.getId());
        if (result > 0) {
            a = false;
            throw new BusinessException("此手机号码已经被绑定");
        }
        if (a) {
            int i = sysUserMapper.updateByPrimaryKeySelective(user);
            userDepartmentInfoMapper.deleteByUserId(user.getId());
            sysUserRoleMapper.deleteUserRoleByUserId(user.getId());
            String[] split = departIds.split(",");
            saveUserDeparts(user.getId(), user.getUpdateUser(), Arrays.asList(split));
            saveUserRole(user, user.getUpdateUser(), Arrays.asList(roleIds.split(",")));
            return i > 0;
        } else {
            return a;
        }
    }


    /**
     * 根据用户编码查询用户
     *
     * @param identityCode
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/getUserByIdentityCode", method = RequestMethod.POST)
    @Override
    public SysUser getUserByIdentityCode(String identityCode) {
        SysUser sysUser = sysUserMapper.getUserByIdentityCode(identityCode);
        if (sysUser != null) {
            sysUser.setPassword(null);
            sysUser.setPasswordRand(null);
        }
        return sysUser;
    }


    /**
     * 重置密码
     *
     * @param userId
     * @param sessionuserId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/resetPassword", method = RequestMethod.POST)
    @Override
    @Transactional
    public void resetPassword(String userId, String sessionuserId) {
        if (null != userId) {
            SysUser user = sysUserMapper.selectByPrimaryKey(userId);
            String randPwd = CommonConstant.INIT_PASSWORD_RAND;
            String pwd = MD5Util.generateMD5("123456", randPwd);
            user.setPassword(pwd);
            user.setPasswordRand(randPwd);
            user.setUpdateUser(sessionuserId);
            user.setUpdateTime(new Date());
            sysUserMapper.updateByPrimaryKeySelective(user);
        } else {
            throw new BusinessException("密码重置失败，userId为空！");
        }
    }

    /**
     * 冻结用户
     *
     * @param userId
     * @param status
     * @param sessionUserId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/freezeUser", method = RequestMethod.POST)
    @Override
    @Transactional
    public void freezeUser(String userId, String status, String sessionUserId) {
        if (null != userId) {
            SysUser user = sysUserMapper.selectByPrimaryKey(userId);
            user.setStatus(Integer.parseInt(status));
            user.setUpdateUser(sessionUserId);
            user.setUpdateTime(new Date());
            sysUserMapper.updateByPrimaryKeySelective(user);
        } else {
            throw new BusinessException("操作失败，userId为空！");
        }
    }

    /**
     * 修改用户信息
     *
     * @param sysUser
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateUserInfo", method = RequestMethod.POST)
    @Override
    public void updateUserInfo(SysUser sysUser) {
        int i = sysUserMapper.updateByPrimaryKeySelective(sysUser);
    }


    /**
     * 修改密码
     *
     * @param userId
     * @param originalPwd
     * @param newPwd
     * @param conPwd
     * @param sessionuserId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updatePassword", method = RequestMethod.POST)
    @Override
    @Transactional
    public void updatePassword(String userId, String originalPwd, String newPwd, String conPwd, String sessionuserId) {
        if (null != userId) {
            newPwd = newPwd.trim();
            conPwd = conPwd.trim();
            originalPwd = originalPwd.trim();
            String randPwd = CommonConstant.INIT_PASSWORD_RAND;
            SysUser user = sysUserMapper.selectByPrimaryKey(userId);
            originalPwd = MD5Util.generateMD5(originalPwd, randPwd);
            if (user.getPassword().equals(originalPwd)) {
                if (newPwd.equals(conPwd)) {
                    newPwd = MD5Util.generateMD5(newPwd, randPwd);
                    user.setPassword(newPwd);
                    user.setPasswordRand(randPwd);
                    user.setUpdateUser(sessionuserId);
                    user.setUpdateTime(new Date());
                    sysUserMapper.updateByPrimaryKeySelective(user);
                } else {
                    //抛出异常  新密码和确认密码 不一致
                    throw new BusinessException("新密码和确认密码 不一致");
                }
            } else {
                //抛出异常  原密码错误！
                throw new BusinessException("原密码错误");
            }
        } else {
            //抛出异常  userId为空！
            throw new BusinessException("userId为空！");
        }
    }

    /**
     * 查询用户信息
     *
     * @param realName
     * @param pageNo
     * @param pageSize
     * @param sidx
     * @param sord
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryContacts", method = RequestMethod.POST)
    @Override
    public GridModel<SysUser> queryContacts(String realName, Integer pageNo, Integer pageSize, String sidx, String sord) {
        String orderString = "create_time";
        PageBounds pageBounds = new PageBounds(pageNo, pageSize, Order.formString(orderString));
        PageList<SysUser> pageList = (PageList<SysUser>) sysUserMapper.queryContacts(realName, pageBounds);
        return new GridModel<SysUser>(pageList);
    }

    /**
     * 用户注册
     *
     * @param user
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/register", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public void register(SysUser user) {
        //前端传的是userName
        user.setMobile(user.getUserName());
        //校验手机号是否被注册
        int count = sysUserMapper.checkMobileAndEmail(CommonConstant.USER_STATUS_YES_ACTIVATION, null, user.getMobile(), null, null);
        if (count > 0) {
            throw new BusinessException("此手机号码已经被注册");
        }
        user.setId(UUIDGenerator.getUUID());
        user.setCreateTime(new Date());
        String password = user.getPassword();//初始密码
        String passwordRand = CommonConstant.INIT_PASSWORD_RAND;//处理秘钥因子
        String md5Password = MD5Util.generateMD5(password, passwordRand);
        user.setPassword(md5Password);
        user.setPasswordRand(passwordRand);
        user.setStatus(CommonConstant.USER_STATUS_YES_ACTIVATION);
        sysUserMapper.insert(user);
        SysUserRole userRole = new SysUserRole();
        userRole.setId(UUIDGenerator.getUUID());
        userRole.setUserId(user.getId());
        userRole.setRoleId(SysRoleEnum.CUSTOMER.name());
        userRole.setCreateTime(new Date());
        sysUserRoleMapper.insert(userRole);
    }

    /**
     * 根据用户名查询用户
     *
     * @param userName
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByAccount", method = RequestMethod.POST)
    @Override
    public SysUser selectByAccount(String userName) {
        Map<String, Object> param = new HashMap<>();
        param.put("loginAccount", userName);
        return sysUserMapper.getUserByAccount(param);
    }
}

