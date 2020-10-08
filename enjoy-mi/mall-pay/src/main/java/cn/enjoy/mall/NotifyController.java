package cn.enjoy.mall;


import cn.enjoy.mall.service.IPayService;
import cn.enjoy.mall.wxsdk.WXPay;
import cn.enjoy.mall.wxsdk.WXPayUtil;
import cn.enjoy.mall.wxsdk.WxPayConfigImpl;
import cn.enjoy.mq.OrderLogSender;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;


/**
 * 支付成功回调
* @author Jack
* @date 2020/9/8
*/
@Controller
//http请求控制类  Contoller
public class NotifyController {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    private IPayService payService;
    @Autowired
    WxPayConfigImpl wxPayConfig;
    @Autowired
    WXPay wxPay;
    @Value("${wx.appId}")
    private String appId = "";
    @Value("${wx.mchId}")
    private String mchId = "";
    @Value("${wx.key}")
    private String partnerKey = "";
    @Value("${wx.certPath}")
    private String certPath = "";
    @Value("${wx.notify_url}")
    private String notify_url = "http://www.weixin.qq.com/wxpay/pay.php";

    @Autowired
    private OrderLogSender secKillSender;

    /**
     * 支付成功后的回调接口
    * @param request
     * @param response
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    @RequestMapping(value = "/wx/notify", produces = "application/json;charset=UTF-8")
    @ResponseBody
    public String payNotifyUrl(HttpServletRequest request, HttpServletResponse response) throws Exception {
        BufferedReader reader = null;

        reader = request.getReader();
        String line = "";
        String xmlString = null;
        StringBuffer inputString = new StringBuffer();

        while ((line = reader.readLine()) != null) {
            inputString.append(line);
        }
        xmlString = inputString.toString();
        request.getReader().close();
        logger.info("----pay callback data---" + xmlString);
        Map<String, String> map = new HashMap<String, String>();
        String result_code = "";
        String return_code = "";
        String out_trade_no = "";
        map = WXPayUtil.xmlToMap(xmlString);
        result_code = map.get("result_code");
        return_code = map.get("return_code");
        logger.info("--------map ------" + JSONObject.toJSONString(map));
        if (return_code.equals("SUCCESS")) {
            if (result_code.equals("SUCCESS")) {
                //异步记录日志，修改订单状态
                secKillSender.send(map.get("out_trade_no"));
//                String payResult = payService.updateByActionId(map.get("out_trade_no"));
//                logger.info("----------payResult:-----" + payResult);
                return "SUCCESS";
            }
        }
        return "fail";

    }

}

