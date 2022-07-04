package com.edui.location.config;

import com.edui.location.util.MemoryIndex;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

/**
 * @Author edui
 * @Date 2022/7/4
 **/
@Configuration
public class LocationConfig {

    @Bean("ipLocation")
    public MemoryIndex getIpLocationMemoryIndex() throws Exception {
        return new MemoryIndex(new ClassPathResource("db"+System.getProperty("file.separator")+"ipv4data.db").getURL().getPath());
    }

    @Bean("phoneLocation")
    public MemoryIndex getPhoneLocationMemoryIndex() throws Exception {
        return new MemoryIndex(new ClassPathResource("db"+System.getProperty("file.separator")+"phonedata.db").getURL().getPath());
    }

}
