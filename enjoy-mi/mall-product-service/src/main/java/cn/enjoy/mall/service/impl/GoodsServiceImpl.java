package cn.enjoy.mall.service.impl;

import cn.enjoy.core.exception.BusinessException;
import cn.enjoy.core.utils.GridModel;
import cn.enjoy.core.utils.StringUtil;
import cn.enjoy.mall.constant.RedisKey;
import cn.enjoy.mall.dao.*;
import cn.enjoy.mall.model.*;
import cn.enjoy.mall.mongo.GoodsDao;
import cn.enjoy.mall.service.IGoodsCategoryService;
import cn.enjoy.mall.service.IGoodsService;
import cn.enjoy.mall.vo.GoodsDetailVo;
import cn.enjoy.mall.vo.GoodsPageVo;
import cn.enjoy.mall.vo.GoodsSpecPriceAttrVo;
import cn.enjoy.mall.vo.GoodsVo;
import com.github.pagehelper.PageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

/**
 * 商品管理
 * @author Jack
 * @date 2018/2/5.
 */
@RestController
//@RequestMapping("/product/mall/service/IGoodsService")
public class GoodsServiceImpl implements IGoodsService {

    @Resource
    private GoodsDao goodsDao;
    @Resource
    private GoodsMapper goodsMapper;
    @Resource
    private GoodsImagesMapper goodsImagesMapper;
    @Resource
    private GoodsAttrMapper goodsAttrMapper;
    @Resource
    private GoodsAttributeMapper goodsAttributeMapper;
    @Resource
    private SpecMapper specMapper;
    @Resource
    private SpecItemMapper specItemMapper;
    @Resource
    private SpecGoodsPriceMapper specGoodsPriceMapper;
    @Resource
    private SpecGoodsImageMapper specGoodsImageMapper;
    @Resource
    private GoodsCategoryMapper goodsCategoryMapper;
    //    @Resource
//    private OrderGoodsMapper orderGoodsMapper;
    @Resource
    private IGoodsCategoryService goodsCategoryService;

    @Value("${goods.pic.domain}")
    private String goodsPicDomain;
    @Resource
    private RedisTemplate redisTemplate;

    @Value("${dfs.url}")
    private String dfsurl;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * 根据goodsId从mongodb中查询商品
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findOneById", method = RequestMethod.POST)
    @Override
    public GoodsVo findOneById(Integer goodsId) {
        return goodsDao.findOneById(goodsId);
    }

    /**
     * 根据specGoodsId从mongodb中查询商品
     *
     * @param specGoodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findOneBySpecGoodsId", method = RequestMethod.POST)
    @Override
    public GoodsVo findOneBySpecGoodsId(Integer specGoodsId) {
        return goodsDao.findOneBySpecGoodsId(specGoodsId);
    }

    /**
     * 根据秒杀商品id查询商品
     *
     * @param killGoodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/findOneByKillGoodsId", method = RequestMethod.POST)
    @Override
    public GoodsVo findOneByKillGoodsId(Integer killGoodsId) {
        return goodsDao.findOneBySpecGoodsId(killGoodsId);
    }

    /**
     * 保存商品信息-【后端】配商品时用到
     *
     * @param goods
     * @return
     */
    //@RequestMapping(value = "/save", method = RequestMethod.POST)
    @Override
    public int save(Goods goods) {
        Integer goodsId = goods.getGoodsId();
//        goods.setOriginalImg("http://" + dfsurl + goods.getOriginalImg());
        if (goodsId != null && goodsId > 0 && goodsMapper.selectByPrimaryKey(goodsId) != null) {
            goodsMapper.updateByPrimaryKeySelective(goods);
            return goodsId;
        } else {
            goodsMapper.insert(goods);
            return goods.getGoodsId();
        }
    }

    /**
     * 删除商品
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/delete", method = RequestMethod.POST)
    @Override
    public int delete(Integer goodsId) {
        return goodsMapper.deleteByPrimaryKey(goodsId);
    }

    /**
     * 批量删除商品
     *
     * @param goodsIds
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/deteteBatch", method = RequestMethod.POST)
    @Override
    public int deteteBatch(Integer[] goodsIds) {
        return goodsMapper.deteteBatch(goodsIds);
    }

    /**
     * 查询商品列表
     *
     * @param catId
     * @param keywords
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
    //@RequestMapping(value = "/queryListPageFromDB", method = RequestMethod.POST)
    @Override
    public GridModel<Goods> queryListPageFromDB(Integer catId, String keywords, int page, int pageSize, String sidx, String sord) {
        String orderString = "goods_id.desc";
        if (StringUtil.isNotEmpty(sidx) && StringUtil.isNotEmpty(sord)) {
            orderString = sidx + "." + sord;
        }
//        PageBounds pageBounds = new PageBounds(page, pageSize, com.github.miemiedev.mybatis.paginator.domain.Order.formString(orderString));
        List<Integer> subCatList = null;
        if (catId != null && catId.intValue() > 0) {
            subCatList = goodsCategoryService.getSubCats(catId);
        }
        PageHelper.startPage(page, pageSize);
        List<Goods> list = goodsMapper.queryListPageFromDB(subCatList, keywords);
//        if(pageList.getPaginator() == null) {
//            Integer totalCount = goodsMapper.queryListPageFromDBTotalCount(subCatList, keywords);
//            Paginator paginator = new Paginator(page,pageSize,totalCount);
//            PageList<Goods> goods = new PageList<>(pageList, paginator);
//            GridModel<Goods> goodsGridModel = new GridModel<>(goods);
//            String goodsGridModelStr = JSONObject.toJSONString(goodsGridModel);
//            return goodsGridModel;
//        }
        GridModel<Goods> goodsGridModel = new GridModel<>(list);
        return goodsGridModel;
    }

    /**
     * 查询商品详情
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryDetailFromDB", method = RequestMethod.POST)
    @Override
    public GoodsDetailVo queryDetailFromDB(Integer goodsId) {
        Goods goods = goodsMapper.selectDetailByGoodsId(goodsId);
        if (goods != null) {
            GoodsDetailVo vo = new GoodsDetailVo();
            vo.setBase(goods);
            vo.setImageList(this.queryGoodsImageFromDB(goodsId));//图片
            vo.setSpecGoodsPriceList(this.querySpecGoodsPriceListFromDB(goodsId));//商品已选的规格列表
            vo.setAttrsList(queryGoodsAttrListFromDB(goodsId));//商品已选属性
            if (goods.getGoodsType() != null) {
                vo.setTypeGoodsAttributeList(queryGoodsAttributeListByGoodsIdAndTypeFromDB(goodsId, goods.getGoodsType()));
                vo.setTypeSpecList(querySpecListByGoodsIdAndTypeIdFromDB(goodsId, goods.getGoodsType()));
            }
            return vo;
        } else {
            return null;
        }
    }

    /**
     * 查询商品信息
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryGoodsFromDB", method = RequestMethod.POST)
    @Override
    public Goods queryGoodsFromDB(Integer goodsId) {
        return goodsMapper.selectByPrimaryKey(goodsId);
    }

    /**
     * 查询商品图片
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryGoodsImageFromDB", method = RequestMethod.POST)
    @Override
    public List<GoodsImages> queryGoodsImageFromDB(Integer goodsId) {
        return goodsImagesMapper.selectByGoodsId(goodsId);
    }

    /**
     * 查询商品规格
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/querySpecGoodsPriceListFromDB", method = RequestMethod.POST)
    @Override
    public List<SpecGoodsPrice> querySpecGoodsPriceListFromDB(Integer goodsId) {
        List<SpecGoodsPrice> list = specGoodsPriceMapper.selectByGoodsId(goodsId);
        for (SpecGoodsPrice spec : list) {
            //4.1 SpecItem
            List<SpecGoodsImage> imageList = specGoodsImageMapper.selectBySpecGoodsId(spec.getId());
            spec.setSpecGoodsImagesList(imageList);
        }
        return list;
    }

    /**
     * 查询商品属性
     *
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryGoodsAttrListFromDB", method = RequestMethod.POST)
    @Override
    public List<GoodsAttr> queryGoodsAttrListFromDB(Integer goodsId) {
        return goodsAttrMapper.selectByGoodsId(goodsId);
    }

    /**
     * 根据typeId查询商品属性
     *
     * @param typeId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryGoodsAttributeListByTypeFromDB", method = RequestMethod.POST)
    @Override
    public List<GoodsAttribute> queryGoodsAttributeListByTypeFromDB(Short typeId) {
        return goodsAttributeMapper.selectListByTypeId(typeId);
    }

    /**
     * 根据goodsId和typeId查询商品属性
     *
     * @param goodsId
     * @param typeId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/queryGoodsAttributeListByGoodsIdAndTypeFromDB", method = RequestMethod.POST)
    @Override
    public List<GoodsAttribute> queryGoodsAttributeListByGoodsIdAndTypeFromDB(Integer goodsId, Short typeId) {
        List<GoodsAttribute> goodsAttributesList = queryGoodsAttributeListByTypeFromDB(typeId);
        if (goodsId != null && goodsId.shortValue() > 0) {
            List<GoodsAttr> goodsAttrList = queryGoodsAttrListFromDB(goodsId);
            for (GoodsAttribute attribute : goodsAttributesList) {
                for (GoodsAttr attr : goodsAttrList) {
                    if (attribute.getAttrId().intValue() == attr.getAttrId().intValue()) {
                        attribute.setAttrValues(attr.getAttrValue());
                        break;
                    }
                }
            }
        }
        return goodsAttributesList;
    }

    /**
     * 根据typeId查询商品规格
     *
     * @param typeId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/querySpecListByTypeFromDB", method = RequestMethod.POST)
    @Override
    public List<Spec> querySpecListByTypeFromDB(Short typeId) {
        List<Spec> typeSpecList = specMapper.selectListByTypeId((int) typeId);
        for (Spec spec : typeSpecList) {
            //4.1 SpecItem
            List<SpecItem> itemList = specItemMapper.selectListBySpecId(spec.getId());
            spec.setSpecItemList(itemList);
        }
        return typeSpecList;
    }

    /**
     * 根据goodsId和typeId查询商品规格
     *
     * @param goodsId
     * @param typeId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/querySpecListByGoodsIdAndTypeIdFromDB", method = RequestMethod.POST)
    @Override
    public List<Spec> querySpecListByGoodsIdAndTypeIdFromDB(Integer goodsId, Short typeId) {
        List<Spec> typeSpecList = querySpecListByTypeFromDB(typeId);
        //获取所有已选的key
        Set<Integer> keyList = null;
        if (goodsId != null && goodsId > 0) {
            keyList = new HashSet<>();
            List<SpecGoodsPrice> specGoodsPriceList = this.querySpecGoodsPriceListFromDB(goodsId);
            for (SpecGoodsPrice price : specGoodsPriceList) {
                String[] keyArray = price.getKey().split("_");
                if (keyArray.length > 0) {
                    for (String str : keyArray) {
                        if (!keyList.contains(str)) {
                            keyList.add(Integer.parseInt(str));
                        }
                    }
                }
            }
        }
        for (Spec spec : typeSpecList) {
            //4.1 SpecItem
            List<SpecItem> itemList = spec.getSpecItemList();
            for (SpecItem item : itemList) {
                if (keyList != null && keyList.size() > 0) {
                    if (keyList.contains(item.getId())) {
                        item.setChecked(true);
                    } else {
                        item.setChecked(false);
                    }
                    /*
                    boolean flag = false;
                    for(Integer key : keyList) {
                        if (item.getId().intValue() == key.intValue()){
                           flag = true;
                           break;
                        }
                    }
                    item.setChecked(flag);
                    */
                } else {
                    item.setChecked(false);
                }
            }
            spec.setSpecItemList(itemList);
        }
        return typeSpecList;
    }

    /**
     * 保存商品图片地址
     *
     * @param imageUrls
     * @param goodsId
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/saveGoodsImages2DB", method = RequestMethod.POST)
    @Transactional
    @Override
    public int saveGoodsImages2DB(String[] imageUrls, Integer goodsId) {
        goodsImagesMapper.deleteByGoodsId(goodsId);
        if (imageUrls != null && imageUrls.length > 0) {
            List<GoodsImages> goodsImagesList = new ArrayList<>();
            for (String url : imageUrls) {
                GoodsImages image = new GoodsImages();
                image.setGoodsId(goodsId);
                image.setImageUrl("http://" + dfsurl + url);
                goodsImagesList.add(image);
            }
            return goodsImagesMapper.insertBatch(goodsImagesList);
        } else {
            return 0;
        }
    }

    /**
     * 保存商品规格和属性
     *
     * @param specPriceAttrVo
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/saveGoodsSpecAndAttr2DB", method = RequestMethod.POST)
    @Transactional
    @Override
    public void saveGoodsSpecAndAttr2DB(GoodsSpecPriceAttrVo specPriceAttrVo) {
        List<SpecGoodsPrice> specGoodsPriceList = specPriceAttrVo.getSpecPriceList();
        List<GoodsAttr> goodsAttrList = specPriceAttrVo.getAttrList();
        Integer goodsId = specPriceAttrVo.getGoodsId();
        Goods goods = new Goods();
        goods.setGoodsId(goodsId);
        goods.setGoodsType(specPriceAttrVo.getGoodsType());
        this.save(goods);
        //规格处理
        List<Integer> specPriceUpdateIdList = null;
        if (!CollectionUtils.isEmpty(specGoodsPriceList)) {
            specPriceUpdateIdList = specGoodsPriceList.stream().filter(x -> x.getId() > 0).map(x -> x.getId()).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(specPriceUpdateIdList)) {
                specPriceUpdateIdList = null;
            }
        }
        specGoodsPriceMapper.deleteByGoodsId(goodsId, specPriceUpdateIdList);
        for (SpecGoodsPrice specGoodsPrice : specGoodsPriceList) {
            if (specGoodsPrice.getId() > 0) {
                specGoodsPriceMapper.updateByPrimaryKey(specGoodsPrice);
            } else {
                specGoodsPriceMapper.insert(specGoodsPrice);
            }
        }
        //specGoodsPriceMapper.insertBatch(specGoodsPriceList);
        /*//
        specGoodsImageMapper.deleteByGoodsId(goodsId);
        //暂时不处理图片
        List<SpecGoodsPrice> savedSpecGoodsPriceList = specGoodsPriceMapper.selectByGoodsId(goodsId);
        List<SpecGoodsImage> specGoodsImageList = new ArrayList<>();
        for (SpecGoodsPrice specPrice : specGoodsPriceList) {
            if (specPrice.getSpecGoodsImagesList() != null) {
                specGoodsImageList.addAll(specPrice.getSpecGoodsImagesList());
            }
        }
        if (!CollectionUtils.isEmpty(specGoodsImageList)) {
            specGoodsImageMapper.insertBatch(specGoodsImageList);
        }*/

        //属性处理
        List<Short> attrUpdateIdList = null;
        if (!CollectionUtils.isEmpty(goodsAttrList)) {
            attrUpdateIdList = goodsAttrList.stream().filter(x -> x.getAttrId() > 0).map(x -> x.getAttrId()).collect(Collectors.toList());
            if (CollectionUtils.isEmpty(attrUpdateIdList)) {
                attrUpdateIdList = null;
            }
        }
        goodsAttrMapper.deleteByGoodsId(goodsId, attrUpdateIdList);
        goodsAttrMapper.insertBatch(goodsAttrList);
        for (GoodsAttr goodsAttr : goodsAttrList) {
            if (goodsAttr.getAttrId() > 0) {
                goodsAttrMapper.updateByPrimaryKey(goodsAttr);
            } else {
                goodsAttrMapper.insert(goodsAttr);
            }
        }
    }

    /**
     * 根据关键字、商品名称查询商品列表
     *
     * @param keyword
     * @return
     */
    //@RequestMapping(value = "/searchList", method = RequestMethod.POST)
    @Override
    public GridModel<GoodsPageVo> searchList(Integer catId, Integer brandId, String order, String keyword, int page, int pageSize) {
        Criteria criteria = new Criteria();
        criteria.and("base.isOnSale").is(true);
        if (catId != null && catId.intValue() > 0) {
            List<Integer> subCates = goodsCategoryService.getSubCats(catId);
            criteria.and("base.catId").in(subCates);
        }
        if (brandId != null && brandId.intValue() > 0) {
            criteria.and("base.brandId").in(brandId);
        }
        if (!StringUtils.isEmpty(keyword)) {
            List<Integer> subCates = goodsCategoryService.getSubCats(keyword);
            //按商品名称、关键字、分类名称来查
            criteria.andOperator(new Criteria().orOperator(Criteria.where("base.goodsName").regex(keyword),
                    Criteria.where("base.keywords").regex(keyword),
                    Criteria.where("base.catId").in(subCates))
            );
        }
        Query query = new Query(criteria);
        if (!StringUtils.isEmpty(order)) {
            String[] orderStr = order.trim().split("\\.");
            if (orderStr.length >= 2) {
                if ("asc".equals(orderStr[1].toLowerCase())) {
                    query.with(Sort.by("base." + orderStr[0]));
                } else if ("desc".equals(orderStr[1].toLowerCase())) {
                    query.with(Sort.by("base." + orderStr[0]));
                }
            }
        }
        query.with(Sort.by("base.sort"));
        query.with(Sort.by("base.sort"));
        query.with(Sort.by("base.sort"));
        return goodsDao.queryPage(page, pageSize, GoodsPageVo.class, query);
    }

    /**
     * 发布所有商品到mongodb
     */
    //@RequestMapping(value = "/publishAll2MongoDB")
    @Override
    public void publishAll2MongoDB() {
        List<Goods> list = goodsMapper.selectAll();
        goodsDao.removeAll();
        publishGoods2MongoDB(list);
    }

    /**
     * 发布单个商品到mongodb
     *
     * @param goodsId
     */
    //@RequestMapping(value = "/publishGoods2MongoDB", method = RequestMethod.POST)
    @Override
    public void publishGoods2MongoDB(Integer goodsId) {
        Goods goods = goodsMapper.selectByPrimaryKey(goodsId);
        if (goods == null) {
            throw new BusinessException("发布失败：该商品不存在");
        }
        goodsDao.remove(goodsId);
        List<SpecGoodsPrice> specGoodsPriceList = specGoodsPriceMapper.selectByGoodsId(goodsId);
        if (CollectionUtils.isEmpty(specGoodsPriceList)) {
            throw new BusinessException("发布失败：该商品没有配置规格价格");
        }
        List<Goods> list = new ArrayList();
        list.add(goods);
        long begin = System.currentTimeMillis();
        publishGoods2MongoDB(list);
        System.out.println("public花费时间：" + (System.currentTimeMillis() - begin));
    }


    private void publishGoods2MongoDB2(List<Goods> list) {
        List<GoodsVo> voList = new ArrayList<>();
        ExecutorService executors = Executors.newFixedThreadPool(50);
        try {
            for (Goods goods : list) {
                GoodsVo vo = new GoodsVo();
                Integer goodsId = goods.getGoodsId();
                //1.base
                vo.setBase(goods);

                Callable<List<GoodsAttr>> attrsListCallable = () -> goodsAttrMapper.selectByGoodsId(goodsId);
                FutureTask<List<GoodsAttr>> attrsListTask = new FutureTask(attrsListCallable);


                Callable<List<GoodsImages>> goodsImagesListCallable = () -> goodsImagesMapper.selectByGoodsId(goodsId);
                FutureTask<List<GoodsImages>> goodsImagesLisTask = new FutureTask<List<GoodsImages>>(goodsImagesListCallable);


                Callable<List<SpecGoodsPrice>> specGoodsPriceListCallable = () -> specGoodsPriceMapper.selectByGoodsId(goodsId);
                FutureTask<List<SpecGoodsPrice>> specGoodsPriceListTask = new FutureTask<List<SpecGoodsPrice>>(specGoodsPriceListCallable);

                Callable<GoodsCategory> goodsCategoryCallable = () -> goodsCategoryMapper.selectByPrimaryKey(goods.getCatId().shortValue());
                FutureTask<GoodsCategory> goodsCategoryTask = new FutureTask<GoodsCategory>(goodsCategoryCallable);


                executors.submit(attrsListTask);
                executors.submit(goodsImagesLisTask);
                executors.submit(goodsCategoryTask);
                executors.submit(specGoodsPriceListTask);

                List<SpecGoodsPrice> specGoodsPriceList = specGoodsPriceListTask.get();
                if (CollectionUtils.isEmpty(specGoodsPriceList)) {
                    continue;
                }
                //4. specGoodsPriceList
                vo.setSpecGoodsPriceList(specGoodsPriceList);

                Callable<List<Spec>> goodsSpecListCallable = () -> {
                    Short typeId = goods.getGoodsType();
                    if (typeId != null) {
                        List<Spec> typeSpecList = specMapper.selectListByTypeId((int) typeId);
                        for (Spec spec : typeSpecList) {
                            //4.1 SpecItem
                            List<SpecItem> itemList = specItemMapper.selectListBySpecId(spec.getId());
                            spec.setSpecItemList(itemList);
                        }
                        List<Spec> goodsSpecList = new ArrayList<>();
                        //Collections.copy(goodsSpecList,typeSpecList);
                        List<String> keyList = new ArrayList<String>();

                        for (SpecGoodsPrice specGoodsPrice : specGoodsPriceList) {
                            List<SpecGoodsImage> specImagesList = specGoodsImageMapper.selectBySpecGoodsId(specGoodsPrice.getId());
                            specGoodsPrice.setSpecGoodsImagesList(specImagesList);
                            String key = specGoodsPrice.getKey();
                            String[] keyArray = key.split("_");
                            if (keyArray.length > 0) {
                                for (String str : keyArray) {
                                    if (!keyList.contains(str)) {
                                        keyList.add(str);
                                    }
                                }
                            }
                        }
                        for (Spec spec : typeSpecList) {
                            List<SpecItem> specItemList = spec.getSpecItemList();
                            boolean findFlag = false;
                            for (SpecItem item : specItemList) {
                                Integer id = item.getId();
                                for (String key : keyList) {
                                    if (String.valueOf(id).equals(key)) {
                                        findFlag = true;
                                        break;
                                    }
                                }
                                if (findFlag) {
                                    break;
                                }
                            }
                            if (findFlag) {
                                goodsSpecList.add(spec);
                            }
                        }
                        return goodsSpecList;
                    }
                    return null;
                };

                FutureTask<List<Spec>> goodsSpecListTask = new FutureTask<List<Spec>>(goodsSpecListCallable);
                executors.submit(goodsSpecListTask);
                vo.setAttrsList(attrsListTask.get());
                vo.setImageList(goodsImagesLisTask.get());
                vo.setGoodsSpecList(goodsSpecListTask.get());
                vo.setCatIdPath(goodsCategoryTask.get().getParentIdPath());
                voList.add(vo);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        goodsDao.insert(voList);
    }

    /**
     * 保存商品到mongodb
     *
     * @param list
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/publishGoods2MongoDB1", method = RequestMethod.POST)
    @Override
    public void publishGoods2MongoDB(List<Goods> list) {
        List<GoodsVo> voList = new ArrayList<>();
        for (Goods goods : list) {
            GoodsVo vo = new GoodsVo();
            Integer goodsId = goods.getGoodsId();
            //1.base
            vo.setBase(goods);
            //2. GoodsAttr
            List<GoodsAttr> attrsList = goodsAttrMapper.selectByGoodsId(goodsId);
            vo.setAttrsList(attrsList);
            //3. goodsImagesList
            List<GoodsImages> goodsImagesList = goodsImagesMapper.selectByGoodsId(goodsId);
            vo.setImageList(goodsImagesList);
            //4. specGoodsPriceList
            List<SpecGoodsPrice> specGoodsPriceList = specGoodsPriceMapper.selectByGoodsId(goodsId);
            if (CollectionUtils.isEmpty(specGoodsPriceList)) {
                continue;
            }
            vo.setSpecGoodsPriceList(specGoodsPriceList);
            //5. goodsSpecList
            Short typeId = goods.getGoodsType();
            if (typeId != null) {
                List<Spec> typeSpecList = specMapper.selectListByTypeId((int) typeId);
                for (Spec spec : typeSpecList) {
                    //4.1 SpecItem
                    List<SpecItem> itemList = specItemMapper.selectListBySpecId(spec.getId());
                    spec.setSpecItemList(itemList);
                }
                List<Spec> goodsSpecList = new ArrayList<>();
                //Collections.copy(goodsSpecList,typeSpecList);
                List<String> keyList = new ArrayList<String>();

                for (SpecGoodsPrice specGoodsPrice : specGoodsPriceList) {
                    List<SpecGoodsImage> specImagesList = specGoodsImageMapper.selectBySpecGoodsId(specGoodsPrice.getId());
                    specGoodsPrice.setSpecGoodsImagesList(specImagesList);
                    String key = specGoodsPrice.getKey();
                    String[] keyArray = key.split("_");
                    if (keyArray.length > 0) {
                        for (String str : keyArray) {
                            if (!keyList.contains(str)) {
                                keyList.add(str);
                            }
                        }
                    }
                }
                for (Spec spec : typeSpecList) {
                    List<SpecItem> specItemList = spec.getSpecItemList();
                    boolean findFlag = false;
                    for (SpecItem item : specItemList) {
                        Integer id = item.getId();
                        for (String key : keyList) {
                            if (String.valueOf(id).equals(key)) {
                                findFlag = true;
                                break;
                            }
                        }
                        if (findFlag) {
                            break;
                        }
                    }
                    if (findFlag) {
                        goodsSpecList.add(spec);
                    }
                }
                vo.setGoodsSpecList(goodsSpecList);
            }
            //6. category parent_id_path
            GoodsCategory goodsCategory = goodsCategoryMapper.selectByPrimaryKey(goods.getCatId().shortValue());
            vo.setCatIdPath(goodsCategory.getParentIdPath());
            voList.add(vo);
        }
        goodsDao.insert(voList);
    }

    /**
     * 查询热销商品
     *
     * @param showNum
     * @return
     */
    //@RequestMapping(value = "/queryHotSellingGoods", method = RequestMethod.POST)
    @Override
    public List<HotSellingGoods> queryHotSellingGoods(Integer showNum) {
        if (showNum == null || showNum <= 0) {
            showNum = 40;
        }
        List<HotSellingGoods> resultList = new ArrayList<>();
        Integer start = 0;
        Integer end = showNum;
        List<HotSellingGoods> hotSellingGoodsList = queryHotSellingGoods(start, end);
        resultList.addAll(hotSellingGoodsList);
        return resultList;
    }

    private List<HotSellingGoods> queryHotSellingGoods(Integer start, Integer end) {
        List<HotSellingGoods> list = new ArrayList<>();
        //1. 从redis中倒序查找指定位置的数据，key是销量，value是goodsId
        Set<ZSetOperations.TypedTuple<String>> set = redisTemplate.opsForZSet().reverseRangeWithScores(RedisKey.GOODS_SALE_NUM, start, end);
        //2. 得到所有的goodsId列表
        List<Integer> goodsIdList = set.stream().map(x -> Integer.parseInt(x.getValue())).collect(Collectors.toList());
        //3. 从mongodb中获取指定goodsId列表的商品信息
        List<GoodsVo> goodsVoList = goodsDao.findList(new Query(new Criteria().and("base.isOnSale").is(true).and("base.goodsId").in(goodsIdList)), GoodsVo.class);
        //4. 对商品列表重新排序，构建返回的商品列表list
        if (!CollectionUtils.isEmpty(goodsVoList)) {
            Iterator<ZSetOperations.TypedTuple<String>> iter = set.iterator();
            while (iter.hasNext()) {
                ZSetOperations.TypedTuple<String> type = iter.next();
                for (GoodsVo goodsVo : goodsVoList) {
                    if (goodsVo != null && goodsVo.getBase().getGoodsId().intValue() == Integer.parseInt(type.getValue())) {
                        HotSellingGoods hotSellingGoods = new HotSellingGoods();
                        hotSellingGoods.setGoodsId(goodsVo.getBase().getGoodsId());
                        hotSellingGoods.setOriginalImg(goodsVo.getBase().getOriginalImg());
                        hotSellingGoods.setShopPrice(goodsVo.getBase().getShopPrice());
                        hotSellingGoods.setMarketPrice(goodsVo.getBase().getMarketPrice());
                        hotSellingGoods.setGoodsName(goodsVo.getBase().getGoodsName());
                        hotSellingGoods.setSalesSum(type.getScore().longValue());
                        if (!CollectionUtils.isEmpty(goodsVo.getSpecGoodsPriceList())) {
                            hotSellingGoods.setSpecGoodsId(goodsVo.getSpecGoodsPriceList().get(0).getId());
                        }
                        list.add(hotSellingGoods);
                        break;
                    }
                }
            }
        }
        return list;
    }

    /**
     * 更新库存
     *
     * @param records
     * @return
     * @throws Exception
     * @author Jack
     * @date 2020/9/8
     * @version
     */
    //@RequestMapping(value = "/updateBySpecGoodsId", method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
    @Override
    public int updateBySpecGoodsIds(List<SpecGoodsPrice> records) {
        String sql = "update tp_spec_goods_price set store_count = store_count-? where id = ? and store_count > 0";
        int[] ints = jdbcTemplate.batchUpdate(sql, new BatchPreparedStatementSetter() {
            @Override
            public void setValues(PreparedStatement ps, int i) throws SQLException {
                ps.setInt(1, records.get(i).getStoreCount());
                ps.setInt(2, records.get(i).getId());
            }

            @Override
            public int getBatchSize() {
                return records.size();
            }
        });
        return ints.length;
    }
}
