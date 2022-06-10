package cn.itcast.tags.platform;

import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@Configuration
@EnableWebMvc
public class AppConfigurer implements WebMvcConfigurer {

    @Override
    public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**")
                .allowCredentials(true)
                .allowedHeaders("*")
//                .allowedOrigins("http://localhost:8080/")
                .allowedOriginPatterns("*")
                .allowedMethods("*");
    }

    @Override
    public void addResourceHandlers(ResourceHandlerRegistry registry) {
        //开放static,templates,public 目录 但是请求时候需要加上对应的前缀,比如我访问static下的资源/static/xxxx/xx.js
        registry.addResourceHandler("/static/**","/templates/**","/public/**")
                .addResourceLocations("classpath:/static/","classpath:/templates/","classpath:/public/");

    }
}
