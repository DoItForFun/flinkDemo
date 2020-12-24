package com.flyAI.flinkJob.LogAnalysis.vo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;

import java.util.Date;

/**
 * @author lizhe
 */
@Data
public class NginxVo {
    @JSONField(name = "@timestamp")
    private Date timestamp;
    private String url;
    private Integer status;
    private String domain;
    @JSONField(name = "remote_addr")
    private String remoteAddr;
    @JSONField(name = "request_body")
    private String requestBody;
    @JSONField(name = "request_uri")
    private String requestUri;
    @JSONField(name = "xff")
    private String xff;
    private String remoteIp;
}
