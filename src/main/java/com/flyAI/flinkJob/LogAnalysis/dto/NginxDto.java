package com.flyAI.flinkJob.LogAnalysis.dto;

import com.flyAI.flinkJob.LogAnalysis.entity.NginxEntity;
import com.flyAI.flinkJob.LogAnalysis.vo.NginxVo;

/**
 * @author lizhe
 */
public class NginxDto {
    public static NginxEntity getNginxEntity(NginxVo nginxVo , long count , long start , long end){
        NginxEntity entity = new NginxEntity();
        entity.setDomain(nginxVo.getDomain());
        entity.setRemoteAddr(nginxVo.getRemoteAddr());
        entity.setRemoteIp(nginxVo.getRemoteIp());
        entity.setRequestBody(nginxVo.getRequestBody());
        entity.setRequestUri(nginxVo.getRequestUri());
        entity.setStatus(nginxVo.getStatus());
        entity.setTimestamp(nginxVo.getTimestamp());
        entity.setUrl(nginxVo.getUrl());
        return entity;
    }
}
