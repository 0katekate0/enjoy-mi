/**
 * Created on 2015年9月2日 by Caiming
 */
package cn.enjoy.users.service.impl;


import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.UUIDGenerator;
import cn.enjoy.sys.model.SelectModel;
import cn.enjoy.sys.model.SysDictData;
import cn.enjoy.sys.model.SysDictType;
import cn.enjoy.sys.service.IDictTypeService;
import cn.enjoy.users.dao.SysDictDataMapper;
import cn.enjoy.users.dao.SysDictTypeMapper;
import com.github.miemiedev.mybatis.paginator.domain.Order;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 字典类型
 *
 * @author Jack
 * @date 2020/9/8
 */
@RestController
//@RequestMapping("/user/sys/service/IDictTypeService")
public class DictTypeServiceImpl implements IDictTypeService {

    @Autowired
    private SysDictDataMapper dictDataMapper;

    @Autowired
    private SysDictTypeMapper dictTypeMapper;

    /**
     * 根据类型查询所有的字典数据
     *
     * @param type
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findDictByType", method = RequestMethod.POST)
    @Override
    public List<SysDictData> findDictByType(String type) {
        return dictDataMapper.queryDictDataByType(type);
    }

    /**
     * 查询所有的字典数据
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findAllDict")
    @Override
    public List<SysDictData> findAllDict() {
        return dictDataMapper.queryAllDictData();
    }

    /**
     * 查询所有的字典类型数据
     *
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findAllDictType")
    @Override
    public List<SysDictType> findAllDictType() {
        return dictTypeMapper.queryAllDictType();
    }

    /**
     * 查询下拉框式的字典类型数据
     *
     * @param type
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findSelectModelsByType", method = RequestMethod.POST)
    @Override
    public List<SelectModel> findSelectModelsByType(String type) {
        return dictDataMapper.querySelectModelByType(type);
    }

    /**
     * 根据类型和排序查询字典数据
     *
     * @param param
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryDictDataByTypeAndORDER", method = RequestMethod.POST)
    @Override
    public List<SysDictData> queryDictDataByTypeAndORDER(Map param) {
        return dictDataMapper.queryDictDataByTypeAndORDER(param);
    }

    /**
     * 查询字典类型列表
     *
     * @param param
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
    //@RequestMapping(value = "/queryAllDictTypePage", method = RequestMethod.POST)
    @Override
    public GridModel<SysDictType> queryAllDictTypePage(String param, Integer pageNo, Integer pageSize, String sidx, String sord) {
        String orderString = "";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        }
        PageBounds pageBounds = new PageBounds(pageNo, pageSize, Order.formString(orderString));
        PageList<SysDictType> pageList = (PageList<SysDictType>) dictTypeMapper.queryAllDictTypePage(param, pageBounds);
        return new GridModel<SysDictType>(pageList);
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
    //@RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    @Override
    public void deleteByIds(List<String> ids) {
        dictTypeMapper.deleteByIds(ids);
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
    //@RequestMapping(value = "/addDictType", method = RequestMethod.POST)
    @Override
    @Transactional
    public void addDictType(SysDictType sysDictType) {
        sysDictType.setId(UUIDGenerator.getUUID());
        dictTypeMapper.insert(sysDictType);
    }

    /**
     * 根据id查询字典类型
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectByPrimaryKey", method = RequestMethod.POST)
    @Override
    public SysDictType selectByPrimaryKey(String id) {
        return dictTypeMapper.selectByPrimaryKey(id);
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
    //@RequestMapping(value = "/deleteDictType", method = RequestMethod.POST)
    @Override
    public void deleteDictType(String id) {
        SysDictType sy = dictTypeMapper.selectByPrimaryKey(id);
        List<SysDictData> list = dictDataMapper.queryDictDataByCode(sy.getType());
        if (list.size() > 0) {
            throw new BusinessException("操作失败，该字典组下面还有字典项，不能删除！");
        }
        dictTypeMapper.deleteByPrimaryKey(id);
    }

    /**
     * 动静字典类型
     *
     * @param id
     * @param status
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/freezeDictTyp", method = RequestMethod.POST)
    @Override
    @Transactional
    public void freezeDictTyp(String id, String status) {
        SysDictType sy = dictTypeMapper.selectByPrimaryKey(id);
        sy.setValid(Integer.parseInt(status));
        dictTypeMapper.updateByPrimaryKeySelective(sy);
    }

    /**
     * 更新字典类型
     *
     * @param sysDictType
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateDictType", method = RequestMethod.POST)
    @Override
    @Transactional
    public void updateDictType(SysDictType sysDictType) {
        dictTypeMapper.updateByPrimaryKeySelective(sysDictType);
    }

}

