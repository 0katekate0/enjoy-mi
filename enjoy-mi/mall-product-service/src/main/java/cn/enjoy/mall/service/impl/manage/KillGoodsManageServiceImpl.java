package cn.enjoy.mall.service.impl.manage;

import cn.enjoy.core.utils.GridModel;
import cn.enjoy.mall.constant.KillConstants;
import cn.enjoy.mall.dao.KillGoodsPriceMapper;
import cn.enjoy.mall.model.KillGoodsPrice;
import cn.enjoy.mall.service.manage.IKillSpecManageService;
import cn.enjoy.mall.vo.KillGoodsSpecPriceDetailVo;
import com.github.miemiedev.mybatis.paginator.domain.PageBounds;
import com.github.miemiedev.mybatis.paginator.domain.PageList;
import com.github.miemiedev.mybatis.paginator.domain.Paginator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementSetter;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

@RestController
//@RequestMapping("/product/mall/service/manage/IKillSpecManageService")
public class KillGoodsManageServiceImpl implements IKillSpecManageService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private KillGoodsPriceMapper killGoodsPriceMapper;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 删除秒杀商品缓存
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/delete", method = RequestMethod.POST)
    @Override
    public int delete(Integer id) {
        //清缓存
        stringRedisTemplate.delete(KillConstants.KILLGOODS_LIST);
        stringRedisTemplate.delete(KillConstants.KILL_GOOD_COUNT + id);
        return killGoodsPriceMapper.deleteByPrimaryKey(id);
    }

    /**
     * 保存秒杀商品并加入缓存
     *
     * @param record
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public int save(KillGoodsPrice record) {
        int ret = killGoodsPriceMapper.insert(record);
        if (ret > 0) {//当前秒杀配置成功，配置秒杀虚拟库存
            final String killGoodCount = KillConstants.KILL_GOOD_COUNT + record.getId();

            //清缓存
            stringRedisTemplate.delete(KillConstants.KILLGOODS_LIST);
            //失效时间
            long expireTime = record.getEndTime().getTime() - System.currentTimeMillis();
            if (expireTime > 0) {
                stringRedisTemplate.opsForValue().set(killGoodCount, record.getKillCount().toString(), expireTime, TimeUnit.MILLISECONDS);
            } else {
                stringRedisTemplate.delete(killGoodCount);
            }
        }
        return ret;
    }

    /**
     * 根据specGoodsId查询秒杀商品数量
     *
     * @param specGoodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/selectCountBySpecGoodId", method = RequestMethod.POST)
    @Override
    public int selectCountBySpecGoodId(Integer specGoodsId) {
        return killGoodsPriceMapper.selectCountBySpecGoodId(specGoodsId);
    }

    /**
     * 根据主键查询秒杀商品
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
    public KillGoodsPrice selectByPrimaryKey(Integer id) {
        return killGoodsPriceMapper.selectByPrimaryKey(id);
    }

    /**
     * 根据specGoodsId查询秒杀商品详情
     *
     * @param specGoodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/detailBySpecGoodId", method = RequestMethod.POST)
    @Override
    public KillGoodsSpecPriceDetailVo detailBySpecGoodId(Integer specGoodsId) {
        return killGoodsPriceMapper.detailBySpecGoodId(specGoodsId);
    }

    /**
     * 根据主键查询秒杀商品详情
     *
     * @param id
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/detailById", method = RequestMethod.POST)
    @Override
    public KillGoodsSpecPriceDetailVo detailById(Integer id) {
        return killGoodsPriceMapper.detail(id);
    }

    /**
     * 更新秒杀商品并刷新缓存
     *
     * @param record
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/update", method = RequestMethod.POST)
    @Override
    public int update(KillGoodsPrice record) {
        int ret = killGoodsPriceMapper.updateByPrimaryKey(record);
        if (ret > 0) {//当前秒杀配置成功，配置秒杀虚拟库存
            flushCache(record);
        }
        return ret;
    }

    /**
     * 更新库存
     *
     * @param record
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateSecKill", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public int updateSecKill(KillGoodsPrice record) {
        // 行锁，库存>0进行扣减，直接就能解决超卖
        int i = killGoodsPriceMapper.updateSecKill(record);
        return i;
    }

    /**
     * 根据specGoodsId更新库存
     *
     * @param record
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateBySpecGoodsId", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public int updateBySpecGoodsId(KillGoodsPrice record) {
        String sql = "update tp_kill_goods_price set kill_count = kill_count-? where spec_goods_id = ? and kill_count > 0";
        int i = jdbcTemplate.update(sql, new PreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps) throws SQLException {
                ps.setInt(1, record.getKillCount());
                ps.setInt(2, record.getSpecGoodsId());
            }
        });
//        int i = killGoodsPriceMapper.updateSecKill(record);
//        if(true)throw new RuntimeException("异常测试");
        return i;
    }

    /**
     * 刷新缓存
     *
     * @param record
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/flushCache", method = RequestMethod.POST)
    @Override
    public void flushCache(KillGoodsPrice record) {
        final String killGoodCount = KillConstants.KILL_GOOD_COUNT + record.getId();

        //清缓存
        stringRedisTemplate.delete(KillConstants.KILLGOODS_LIST);
        //失效时间
        long expireTime = record.getEndTime().getTime() - System.currentTimeMillis();
        if (expireTime > 0) {
            stringRedisTemplate.opsForValue().set(killGoodCount, record.getKillCount().toString(), expireTime, TimeUnit.MILLISECONDS);
        } else {
            stringRedisTemplate.delete(killGoodCount);
        }
    }

    /**
     * 查询秒杀商品列表
     *
     * @param name
     * @param page
     * @param pageSize
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryByPage", method = RequestMethod.POST)
    @Override
    public GridModel<KillGoodsSpecPriceDetailVo> queryByPage(String name, int page, int pageSize) {
        PageBounds pageBounds = new PageBounds(page, pageSize);

        PageList<KillGoodsSpecPriceDetailVo> list = killGoodsPriceMapper.select(name, pageBounds);

        if (list.getPaginator() == null) {
            Integer totalCount = killGoodsPriceMapper.selectTotalCount(name);
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            list = new PageList<>(list, paginator);
            GridModel<KillGoodsSpecPriceDetailVo> list2Model = new GridModel<>(list);
            return list2Model;
        }

        return new GridModel<>(list);
    }

    /**
     * 查询秒杀商品列表
     *
     * @param page
     * @param pageSize
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryView", method = RequestMethod.POST)
    @Override
    public GridModel<KillGoodsSpecPriceDetailVo> queryView(int page, int pageSize) {
        PageBounds pageBounds = new PageBounds(page, pageSize);

        PageList<KillGoodsSpecPriceDetailVo> list = killGoodsPriceMapper.selectView(pageBounds);

        if (list.getPaginator() == null) {
            Integer totalCount = killGoodsPriceMapper.selectViewTotalCount();
            Paginator paginator = new Paginator(page, pageSize, totalCount);
            list = new PageList<>(list, paginator);
            GridModel<KillGoodsSpecPriceDetailVo> list2Model = new GridModel<>(list);
            return list2Model;
        }

        return new GridModel<>(list);
    }
}
