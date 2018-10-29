package com.pandatv.bean;

import java.io.Serializable;
import java.util.*;

/**
 * @author: likaiqing
 * @create: 2018-10-22 14:25
 **/
public class RankProject implements Serializable {
    private String project;
    private long startTimeU;
    private long endTimeU;
    private List<String> cates = new ArrayList<>();
    private List<String> giftIds = new ArrayList<>();//礼物id
    private boolean allRank;
    private boolean specificRank;
    private boolean hourAllRank;
    private boolean dayAllRank;
    private boolean weekAllRank;
    private boolean hourSpecificRank;
    private boolean daySpecificRank;
    private boolean weekSpecificRank;
    private int flag;//0:开播有记录即统计，1:报名方式，hostpool:{project},2:分组报名方式，hostpool:{project}:groups用来存组表示，hostpool:{project}:group:{group}只有此处用-分隔，兼容业务
    private Set<String> qids = new HashSet<>();
    //    private Map<String, Set<String>> groupAnchorsMap = new HashMap<>();//每个组对应的主播列表
    private Map<String, String> anchor2GroupMap = new HashMap<>();//主播对应的组


    public boolean isAllRank() {
        return allRank;
    }

    public void setAllRank(boolean allRank) {
        this.allRank = allRank;
    }

    public boolean isSpecificRank() {
        return specificRank;
    }

    public void setSpecificRank(boolean specificRank) {
        this.specificRank = specificRank;
    }

    public boolean isHourAllRank() {
        return hourAllRank;
    }

    public void setHourAllRank(boolean hourAllRank) {
        this.hourAllRank = hourAllRank;
    }

    public boolean isDayAllRank() {
        return dayAllRank;
    }

    public void setDayAllRank(boolean dayAllRank) {
        this.dayAllRank = dayAllRank;
    }

    public boolean isWeekAllRank() {
        return weekAllRank;
    }

    public void setWeekAllRank(boolean weekAllRank) {
        this.weekAllRank = weekAllRank;
    }

    public boolean isHourSpecificRank() {
        return hourSpecificRank;
    }

    public void setHourSpecificRank(boolean hourSpecificRank) {
        this.hourSpecificRank = hourSpecificRank;
    }

    public boolean isDaySpecificRank() {
        return daySpecificRank;
    }

    public void setDaySpecificRank(boolean daySpecificRank) {
        this.daySpecificRank = daySpecificRank;
    }

    public boolean isWeekSpecificRank() {
        return weekSpecificRank;
    }

    public void setWeekSpecificRank(boolean weekSpecificRank) {
        this.weekSpecificRank = weekSpecificRank;
    }

    public Map<String, String> getAnchor2GroupMap() {
        return anchor2GroupMap;
    }

    public void setAnchor2GroupMap(Map<String, String> anchor2GroupMap) {
        this.anchor2GroupMap = anchor2GroupMap;
    }

    public Set<String> getQids() {
        return qids;
    }

    public void setQids(Set<String> qids) {
        this.qids = qids;
    }

    public int getFlag() {
        return flag;
    }

    public void setFlag(int flag) {
        this.flag = flag;
    }

    public long getStartTimeU() {
        return startTimeU;
    }

    public void setStartTimeU(long startTimeU) {
        this.startTimeU = startTimeU;
    }

    public long getEndTimeU() {
        return endTimeU;
    }

    public void setEndTimeU(long endTimeU) {
        this.endTimeU = endTimeU;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public List<String> getCates() {
        return cates;
    }

    public void setCates(List<String> cates) {
        this.cates = cates;
    }

    public List<String> getGiftIds() {
        return giftIds;
    }

    public void setGiftIds(List<String> giftIds) {
        this.giftIds = giftIds;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RankProject that = (RankProject) o;
        return startTimeU == that.startTimeU &&
                endTimeU == that.endTimeU &&
                allRank == that.allRank &&
                specificRank == that.specificRank &&
                hourAllRank == that.hourAllRank &&
                dayAllRank == that.dayAllRank &&
                weekAllRank == that.weekAllRank &&
                hourSpecificRank == that.hourSpecificRank &&
                daySpecificRank == that.daySpecificRank &&
                weekSpecificRank == that.weekSpecificRank &&
                flag == that.flag &&
                Objects.equals(project, that.project) &&
                Objects.equals(cates, that.cates) &&
                Objects.equals(giftIds, that.giftIds) &&
                Objects.equals(qids, that.qids) &&
                Objects.equals(anchor2GroupMap, that.anchor2GroupMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project);
    }

    @Override
    public String toString() {
        return "RankProject{" +
                "startTimeU=" + startTimeU +
                ", endTimeU=" + endTimeU +
                ", project='" + project + '\'' +
                ", cates=" + cates +
                ", giftIds=" + giftIds +
                '}';
    }

    public static void main(String[] args) {
        RankProject rank1 = new RankProject();
        RankProject rank2 = new RankProject();
        rank1.setProject("test");
        rank2.setProject("test");
        HashSet<String> set1 = new HashSet<>();
        set1.add("set1");
        HashSet<String> set2 = new HashSet<>();
        set2.add("set1");
        rank1.setQids(set1);
        rank2.setQids(set2);
        List<String> cates1 = new ArrayList<>();
        cates1.add("cate");
        List<String> cates2 = new ArrayList<>();
        cates2.add("cate");
        rank1.setCates(cates1);
        rank2.setCates(cates2);
        List<String> giftIds1 = new ArrayList<>();
        giftIds1.add("giftId");
        List<String> giftIds2 = new ArrayList<>();
        giftIds2.add("giftId");
        rank1.setGiftIds(giftIds1);
        rank2.setGiftIds(giftIds2);
        rank1.setAllRank(true);
        rank2.setAllRank(true);
        Map<String, String> anchor2GroupMap1 = new HashMap<>();
        anchor2GroupMap1.put("qid1", "qid1");
        Map<String, String> anchor2GroupMap2 = new HashMap<>();
        anchor2GroupMap2.put("qid1", "qid1");
        rank1.setAnchor2GroupMap(anchor2GroupMap1);
        rank2.setAnchor2GroupMap(anchor2GroupMap2);
        boolean equals = rank1.equals(rank2);
        System.out.println(equals);
    }
}
