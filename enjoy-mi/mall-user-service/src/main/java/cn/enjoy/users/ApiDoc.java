package cn.enjoy.users;

import io.github.yedaxia.apidocs.Docs;
import io.github.yedaxia.apidocs.DocsConfig;

/**
 * @Classname ApiDoc
 * @Description 用于生产API文档的
 * @Author Jack
 * Date 2020/8/4 13:31
 * Version 1.0
 */
public class ApiDoc {
    public static void main(String[] args) {
        //开始API的初始化工作
        DocsConfig config= new DocsConfig();
        config.setProjectPath("F:\\xiangxueedu\\project\\mall-springcloud\\mall-user-service"); // 项目根目录
        config.setProjectName("享学课堂-小米商城项目-mall-user-service"); // 项目名称
        config.setApiVersion("V1.0");  // 声明该API的版本
        config.setDocsPath("F:\\xiangxueedu\\project\\mall-springcloud\\mall-user-service"); // 生成API 文档所在目录
        config.setAutoGenerate(Boolean.TRUE);  // 配置自动生成
        config.setMvcFramework("spring");
        Docs.buildHtmlDocs(config); // 执行生成文档
    }
}
