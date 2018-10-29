package com.pandatv.bean;

import java.io.Serializable;
import java.util.Objects;

/**
 * @author: likaiqing
 * @create: 2018-10-25 20:11
 **/
public class GiftInfo implements Serializable {
    private String uid;
    private String qid;
    private String roomId;
    private String giftId;
    private String total;
    private String cate;
    private long timeU;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getQid() {
        return qid;
    }

    public void setQid(String qid) {
        this.qid = qid;
    }

    public String getRoomId() {
        return roomId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }

    public String getGiftId() {
        return giftId;
    }

    public void setGiftId(String giftId) {
        this.giftId = giftId;
    }

    public String getTotal() {
        return total;
    }

    public void setTotal(String total) {
        this.total = total;
    }

    public String getCate() {
        return cate;
    }

    public void setCate(String cate) {
        this.cate = cate;
    }

    public long getTimeU() {
        return timeU;
    }

    public void setTimeU(long timeU) {
        this.timeU = timeU;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GiftInfo giftInfo = (GiftInfo) o;
        return timeU == giftInfo.timeU &&
                Objects.equals(uid, giftInfo.uid) &&
                Objects.equals(qid, giftInfo.qid) &&
                Objects.equals(roomId, giftInfo.roomId) &&
                Objects.equals(giftId, giftInfo.giftId) &&
                Objects.equals(total, giftInfo.total) &&
                Objects.equals(cate, giftInfo.cate);
    }

    @Override
    public int hashCode() {

        return Objects.hash(uid, qid, roomId, giftId, total, cate, timeU);
    }

    @Override
    public String toString() {
        return "GiftInfo{" +
                "uid='" + uid + '\'' +
                ", qid='" + qid + '\'' +
                ", roomId='" + roomId + '\'' +
                ", giftId='" + giftId + '\'' +
                ", total='" + total + '\'' +
                ", cate='" + cate + '\'' +
                ", timeU=" + timeU +
                '}';
    }
}
