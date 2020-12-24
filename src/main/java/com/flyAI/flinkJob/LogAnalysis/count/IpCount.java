package com.flyAI.flinkJob.LogAnalysis.count;

import java.util.Date;

/**
 * @author lizhe
 */
public class IpCount {
    public String remoteIp;
    public long count;

    public Date getStart() {
        return start;
    }

    public void setStart(Date start) {
        this.start = start;
    }

    public Date getEnd() {
        return end;
    }

    public void setEnd(Date end) {
        this.end = end;
    }

    private Date start;
    private Date end;



    public IpCount(){}
    public IpCount(String ip, long count , Date time) {
        this.remoteIp = ip;
        this.count = count;
        this.start = time;
    }

    @Override
    public String toString() {
        return "IpCount{" +
                "remoteIp='" + remoteIp + '\'' +
                ", com.flyAI.flinkJob.LogAnalysis.count=" + count +
                ", start=" + start +
                ", end=" + end +
                '}';
    }
}
