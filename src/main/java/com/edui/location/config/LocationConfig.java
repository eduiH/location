package com.edui.location.config;

import com.edui.location.util.MemoryIndex;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

/**
 * @author edui
 * @date 2022/7/4
 **/
@Configuration
public class LocationConfig {

    @Bean("ipLocation")
    public MemoryIndex getIpLocationMemoryIndex() throws Exception {
        File tempFile = File.createTempFile("indexFile", ".db");
        try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
             InputStream sourceInputStream = new ClassPathResource("db" + System.getProperty("file.separator") + "ipv4data.db").getInputStream()) {
            fileOutputStream.write(sourceInputStream.readAllBytes());
        }
        MemoryIndex memoryIndex = new MemoryIndex(tempFile.getPath());
        tempFile.deleteOnExit();
        return memoryIndex;
    }

    @Bean("phoneLocation")
    public MemoryIndex getPhoneLocationMemoryIndex() throws Exception {
        File tempFile = File.createTempFile("indexFile", ".db");
        try (FileOutputStream fileOutputStream = new FileOutputStream(tempFile);
             InputStream sourceInputStream = new ClassPathResource("db" + System.getProperty("file.separator") + "ipv4data.db").getInputStream()) {
            fileOutputStream.write(sourceInputStream.readAllBytes());
        }
        MemoryIndex memoryIndex = new MemoryIndex(tempFile.getPath());
        tempFile.deleteOnExit();
        return memoryIndex;
    }

}
