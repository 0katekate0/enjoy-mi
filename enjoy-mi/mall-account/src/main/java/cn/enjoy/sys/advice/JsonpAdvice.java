package cn.enjoy.sys.advice;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.support.spring.FastJsonHttpMessageConverter;
import com.fasterxml.jackson.databind.util.JSONPObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.MethodParameter;
import org.springframework.http.MediaType;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.servlet.mvc.method.annotation.ResponseBodyAdvice;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.regex.Pattern;


@Slf4j
@Component
@ControllerAdvice(basePackages = "cn.enjoy.sys.controller")
public class JsonpAdvice extends FastJsonHttpMessageConverter implements ResponseBodyAdvice {

    private static final Pattern CALLBACK_PARAM_PATTERN = Pattern.compile("[0-9A-Za-z_\\.]*");
    public static final Charset UTF8 = Charset.forName("UTF-8");
    private Charset charset;
    private SerializerFeature[] features;

    @Override
    public Object beforeBodyWrite(Object o, MethodParameter methodParameter, MediaType mediaType, Class aClass, ServerHttpRequest serverHttpRequest, ServerHttpResponse serverHttpResponse) {

        HttpServletRequest servletRequest = ((ServletServerHttpRequest) serverHttpRequest).getServletRequest();
        HttpServletResponse response = ((ServletServerHttpResponse) serverHttpResponse).getServletResponse();
        String value = servletRequest.getParameter("callback");
        if (value != null) {
            if (this.isValidJsonpQueryParam(value)) {
                JSONPObject jsonp = new JSONPObject(value, o);
                String text = JSON.toJSONString(jsonp.getValue(), this.features);
                String jsonpText = new StringBuilder(jsonp.getFunction()).append("(").append(text).append(")").toString();
                byte[] bytes = jsonpText.getBytes(this.charset);
                OutputStream out = null;
                try {
                    out = response.getOutputStream();
                    out.write(bytes);
                    out.flush();
                    out.close();
                } catch (IOException e) {

                }
            }
            if (this.logger.isDebugEnabled()) {
                this.logger.debug("Ignoring invalid jsonp parameter value: " + value);
            }
        }
        return o;
    }


    @Override
    public boolean supports(MethodParameter methodParameter, Class aClass) {
        return true;
    }

    protected boolean isValidJsonpQueryParam(String value) {
        return CALLBACK_PARAM_PATTERN.matcher(value).matches();
    }

    public JsonpAdvice() {
        super();
        this.charset = UTF8;
        this.features = new SerializerFeature[0];
    }
}
