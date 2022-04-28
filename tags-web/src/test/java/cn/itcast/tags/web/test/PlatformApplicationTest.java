package cn.itcast.tags.web.test;

import cn.itcast.tags.platform.PlatformApplication;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
@SpringBootApplication
public class PlatformApplicationTest {

    public static void main(String[] args) {
        SpringApplication.run(PlatformApplication.class, args);
    }

}
