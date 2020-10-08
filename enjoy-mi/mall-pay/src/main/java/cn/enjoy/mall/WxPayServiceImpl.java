package cn.enjoy.mall;


import cn.enjoy.mall.service.IWxPayService;
import cn.enjoy.mall.wxsdk.WXPay;
import cn.enjoy.mall.wxsdk.WxPayConfigImpl;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.web.bind.annotation.RestController;

import java.math.BigDecimal;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;


/**
 * 微信预支付
* @author Jack
* @date 2020/9/8
*/
@RefreshScope
@RestController
//@RequestMapping("/pay/mall/service/IWxPayService")
public class WxPayServiceImpl implements IWxPayService {

    private Logger logger = LoggerFactory.getLogger(getClass());

    @Autowired
    WxPayConfigImpl wxPayConfig;
    @Autowired
    WXPay wxPay;

    @Value("${wx.appId}")
    private String appId = "";
    @Value("${wx.mchId}")
    private String mchId = "";
    @Value("${wx.key}")
    private String key = "";
    @Value("${wx.certPath}")
    private String certPath = "";
    @Value("${wx.notify_url}")
    private String notify_url = "";
    @Value("${wx.wap_url}")
    private String wap_url = "";
    @Value("${wx.spbill_create_ip}")
    private String spbill_create_ip = "";

//    @PostConstruct
//    public void tet() {
//        String appId = this.appId;
//    }

    /**
     * 微信预支付订单
    * @param actionId
     * @param payAmount
     * @param userId
    * @author Jack
    * @date 2020/9/8
    * @throws Exception
    * @return
    * @version
    */
    //@RequestMapping(value = "/unifiedorder", method = RequestMethod.POST)
    @Override
    public Map<String, String> unifiedorder(String actionId, BigDecimal payAmount, String userId) {
        Map<String, String> reqData = new HashMap<>();
        try {
            WXPay wxPay = new WXPay(wxPayConfig);

            reqData.put("body", "orderno:" + actionId);
            reqData.put("out_trade_no", actionId);

            reqData.put("total_fee", String.valueOf(payAmount.multiply(BigDecimal.valueOf(100)).intValue()));
            reqData.put("spbill_create_ip", getSpbill_create_ip());
            reqData.put("notify_url", notify_url);
            reqData.put("trade_type", "NATIVE");

            logger.info("--------unifiedorder:" + JSONObject.toJSONString(reqData));
            //调官方sdk统一下单方法
            Map<String, String> result = wxPay.unifiedOrder(reqData);
            logger.info("--------result:" + result);
            return result;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private String getSpbill_create_ip() {
        String ip = "";
        try {
            Enumeration<?> e1 = NetworkInterface.getNetworkInterfaces();//获取多个网卡
            while (e1.hasMoreElements()) {
                NetworkInterface ni = (NetworkInterface) e1.nextElement();

                if (("eth0").equals(ni.getName())) {//取“eth0”网卡
                    Enumeration<?> e2 = ni.getInetAddresses();
                    while (e2.hasMoreElements()) {
                        InetAddress ia = (InetAddress) e2.nextElement();
                        if (ia instanceof Inet6Address) {//排除IPv6地址
                            continue;
                        }
                        ip = ia.getHostAddress();
                    }
                    break;
                }
            }
        } catch (SocketException e) {
            e.printStackTrace();
        }
        logger.info("----------ip:----" + ip);
        return ip;
    }
}
