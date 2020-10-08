package cn.enjoy.mall.web.controller;

import cn.enjoy.core.utils.response.HttpResponseBody;
import cn.enjoy.mall.service.IPayService;
import cn.enjoy.sys.controller.BaseController;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
/*
* 支付服务
* */
@RestController
@RequestMapping("/api/pay")
public class PayController extends BaseController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private IPayService payService;

    /**
     * 微信支付
    * @param orderId
     * @param payCode
     * @param payAmount
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("wxPrePay")
    public HttpResponseBody wxPrePay(Long orderId, String payCode, BigDecimal payAmount){
        Map<String, String> preMap = payService.doPrePay(orderId,payCode,payAmount,getSessionUserId());
        if("success".equalsIgnoreCase(preMap.get("result_code"))){
            return HttpResponseBody.successResponse("生成预支付单成功",preMap);
        }else{
            return HttpResponseBody.failResponse(preMap.get("return_msg"));
        }
    }

    /**
     * 校验是否支付成功
    * @param prepayId
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("checkPay")
    public HttpResponseBody checkPay(String prepayId){
        String payStatus = payService.queryByPrepayId(prepayId);
        Map<String, String> preMap = new HashMap<>();
        preMap.put("payStatus",payStatus);
        return HttpResponseBody.successResponse("支付成功",preMap);
    }

    /**
     * 插入支付订单数据
    * @param orderId
     @param payCode
     @param payAmount
    * @author Jack
    * @date 2020/8/4
    * @throws Exception
    * @return
    * @version
    */
    @PostMapping("orderPay")
    public HttpResponseBody orderPay(Long orderId, String payCode, BigDecimal payAmount){
        String payResult = payService.doPay(orderId,payCode,payAmount,getSessionUserId());
        logger.info("----------payResult-------" + payResult);

        JSONObject jsonObject = JSONObject.parseObject(payResult);
        String respCode = jsonObject.getString("respCode");
        if("0".equals(respCode)){
            return HttpResponseBody.successResponse("支付成功");
        }else{
            return HttpResponseBody.failResponse(payResult);
        }
    }
}
