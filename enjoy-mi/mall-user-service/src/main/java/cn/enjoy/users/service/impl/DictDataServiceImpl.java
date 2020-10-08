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
import cn.enjoy.sys.service.IDictDataService;
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
 * 字典数据管理
 *
 * @author Jack
 * @version 1.0
 */
@RestController
//@RequestMapping("/user/sys/dictData")
public class DictDataServiceImpl implements IDictDataService {

    @Autowired
    private SysDictDataMapper dictDataMapper;

    @Autowired
    private SysDictTypeMapper dictTypeMapper;

    /**
     * 根据字典类型查询字典数据
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
     * 根据编码查询字典数据
     *
     * @param code
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryDictDataByCode", method = RequestMethod.POST)
    @Override
    public List<SysDictData> queryDictDataByCode(String code) {
        return dictDataMapper.queryDictDataByCode(code);
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
     * 查询所有的字段类型
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
     * 查询下拉框的字典数据
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
     * 根据类型查询字典数据
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
     * 查询字典数据列表
     *
     * @param param
     * @param type
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
    @Override
    //@RequestMapping(value = "/queryAllDictDataPage", method = RequestMethod.POST)
    public GridModel<SysDictData> queryAllDictDataPage(String param, String type, Integer pageNo, Integer pageSize, String sidx, String sord) {
        if (null == type) {
            throw new BusinessException("操作失败，type为空");
        }
        String orderString = "";
        if (!StringUtils.isEmpty(sidx)) {
            orderString = sidx + "." + sord;
        }
        PageBounds pageBounds = new PageBounds(pageNo, pageSize, Order.formString(orderString));
        PageList<SysDictData> pageList = (PageList<SysDictData>) dictDataMapper.queryAllDictDataPage(param, type, pageBounds);
        return new GridModel<SysDictData>(pageList);
    }

    /**
     * 删除字典数据
     *
     * @param ids
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
   // @RequestMapping(value = "/deleteByIds", method = RequestMethod.POST)
    @Override
    public void deleteByIds(List<String> ids) {
        dictDataMapper.deleteByIds(ids);
    }

    /**
     * 新增字典数据
     *
     * @param sysDictData
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/addDictData", method = RequestMethod.POST)
    @Override
    @Transactional
    public void addDictData(SysDictData sysDictData) {
        sysDictData.setId(UUIDGenerator.getUUID());
        dictDataMapper.insert(sysDictData);
    }

    /**
     * 根据id查询字典数据
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
    public SysDictData selectByPrimaryKey(String id) {
        return dictDataMapper.selectByPrimaryKey(id);
    }

    /**
     * 根据id删除字典数据
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/deleteDictData", method = RequestMethod.POST)
    @Override
    @Transactional
    public void deleteDictData(String id) {
        dictDataMapper.deleteByPrimaryKey(id);

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
    //@RequestMapping(value = "/freezeDictData", method = RequestMethod.POST)
    @Override
    @Transactional
    public void freezeDictData(String id, String status) {
        SysDictData da = dictDataMapper.selectByPrimaryKey(id);
        da.setValid(Integer.parseInt(status));
        dictDataMapper.updateByPrimaryKey(da);

    }

    /**
     * 更新字典数据
     *
     * @param sysDictData
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateDictData", method = RequestMethod.POST)
    @Override
    @Transactional
    public void updateDictData(SysDictData sysDictData) {
        dictDataMapper.updateByPrimaryKey(sysDictData);

    }


}

