package cn.enjoy.mall.service.manage;

import cn.enjoy.mall.model.Brand;
import org.springframework.web.bind.annotation.RequestMapping;

import java.util.List;

@RequestMapping("/product/mall/service/manage/IBrandService")
public interface IBrandService {
    @RequestMapping(value = "/getAll")
    List<Brand> getAll();
}
