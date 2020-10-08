package cn.enjoy.config;

import cn.enjoy.mall.dao.GoodsMapper;
import cn.enjoy.mall.model.Goods;
import cn.enjoy.mall.mongo.GoodsDao;
import cn.enjoy.mall.service.IGoodsCategoryService;
import cn.enjoy.mall.service.IGoodsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 缓存预热，把商品数据加载到mongodb中
 * @Classname PreLoadGoodsToMongo
 * @Description TODO
 * @Author Jack
 * Date 2020/9/1 11:02
 * Version 1.0
 */
@Component
public class PreLoadGoodsToMongo implements ApplicationListener<ContextRefreshedEvent> {

    @Autowired
    private GoodsMapper goodsMapper;

    @Autowired
    private GoodsDao goodsDao;

    @Autowired
    private IGoodsService goodsService;

    @Autowired
    private IGoodsCategoryService goodsCategoryService;

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        loadGoodsToMongo();
        loadCategory();
    }

    private void loadGoodsToMongo() {
        List<Goods> list = goodsMapper.selectAll();
        goodsDao.removeAll();
        goodsService.publishGoods2MongoDB(list);
    }

    private void loadCategory() {
        goodsCategoryService.produceHotCategories();
        goodsCategoryService.produceCategory4Home();
        goodsCategoryService.produceCategoryGoodsCount();
    }
}
