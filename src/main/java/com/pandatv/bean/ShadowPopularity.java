package com.pandatv.bean;

import java.util.Objects;

/**
 * @author: likaiqing
 * @create: 2018-11-26 16:15
 **/
public class ShadowPopularity {

    private String hostId;
    private String roomId;
    private String cate;
    private int num;
    private long timestamp;
    private String time;

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public String getRoomId() {
        return roomId;
    }

    public void setRoomId(String roomId) {
        this.roomId = roomId;
    }

    public String getCate() {
        return cate;
    }

    public void setCate(String cate) {
        this.cate = cate;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getTime() {
        return time;
    }

    public void setTime(String time) {
        this.time = time;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShadowPopularity that = (ShadowPopularity) o;
        return num == that.num &&
                timestamp == that.timestamp &&
                Objects.equals(hostId, that.hostId) &&
                Objects.equals(roomId, that.roomId) &&
                Objects.equals(cate, that.cate) &&
                Objects.equals(time, that.time);
    }

    @Override
    public int hashCode() {

        return Objects.hash(hostId, roomId, cate, num, timestamp, time);
    }

    @Override
    public String toString() {
        return "ShadowPopularity{" +
                "hostId='" + hostId + '\'' +
                ", roomId='" + roomId + '\'' +
                ", cate='" + cate + '\'' +
                ", num=" + num +
                ", timestamp=" + timestamp +
                ", time='" + time + '\'' +
                '}';
    }
}
