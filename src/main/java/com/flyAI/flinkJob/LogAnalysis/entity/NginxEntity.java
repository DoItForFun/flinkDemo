package com.flyAI.flinkJob.LogAnalysis.entity;

import lombok.Data;

import java.util.Date;

/**
 * @author lizhe
 */
@Data
public class NginxEntity {
    private Date timestamp;
    private String url;
    private Integer status;
    private String domain;
    private String remoteAddr;
    private String requestBody;
    private String requestUri;
    private String remoteIp;
    private long count;
    private long start;
    private long end;
}
