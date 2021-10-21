package com.ctrip.cdc;

import java.util.Date;

/**
 * write ck bean
 *
 * @author leon
 * @date 2021/09/30
 */
public class DataBean {

    private String content;
    private Integer op;
    private String index;
    private Date ts;

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }

    public Integer getOp() {
        return op;
    }

    public void setOp(Integer op) {
        this.op = op;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public Date getTs() {
        return ts;
    }

    public void setTs(Date ts) {
        this.ts = ts;
    }
}
