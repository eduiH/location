package com.edui.location.controller;

import com.edui.location.util.MemoryIndex;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 * @author edui
 * @date 2022/7/4
 **/
@RestController
public class LocationExampleController {


    @Resource(name = "ipLocation")
    private MemoryIndex ipLocationMemoryIndex;

    @Resource(name = "phoneLocation")
    private MemoryIndex phoneMemoryIndex;

    @GetMapping("/ipLocation")
    public String ipLocation(String ip) throws Exception {
        return ipLocationMemoryIndex.findIpAddr(ip);
    }

    @GetMapping("/phoneLocation")
    public String phoneLocation(String phone) throws Exception {
        return phoneMemoryIndex.findPhoneAddr(phone);
    }

    @GetMapping("/currentIpLocation")
    public String currentIpLocation(HttpServletRequest httpServletRequest) throws Exception {
        return ipLocationMemoryIndex.findIpAddr(getIpAddr(httpServletRequest));
    }

    public String getIpAddr(HttpServletRequest request) {
        String ip = request.getHeader("x-forwarded-for");
        if (ip != null && ip.length() != 0 && !"unknown".equalsIgnoreCase(ip)) {
            // 多次反向代理后会有多个ip值，第一个ip才是真实ip
            if (ip.indexOf(",") != -1) {
                ip = ip.split(",")[0];
            }
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_CLIENT_IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.length() == 0 || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        return ip;
    }

}
