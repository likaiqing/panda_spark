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
    //人气榜单
    private boolean popularRank;//代表的是活动期间人气榜单
    private boolean weekPopularRank;//代表的是活动期间按周统计的人气榜单
    private boolean monthPopularRank;//代表的是活动期间按月统计的人气榜单

    //特殊礼物额外多算,比如特殊礼物统计时，猫币多算0.5，即特殊礼物送100猫币，需要统计150猫币,specificExtraValue=0.5
    //一旦设置，日榜总榜，周榜总榜，月榜总榜全部需要多加
    private boolean specificExtraAdd;
    private double specificExtraValue;

    private boolean allRank;//代表的是活动期间的礼物榜单
    private boolean specificRank;//代表活动期间的特殊礼物榜单
    private boolean hourAllRank;//代表活动期间按小时统计礼物榜单
    private boolean dayAllRank;//代表活动期间按天统计的礼物榜单
    private boolean weekAllRank;//代表活动期间按周统计的礼物榜单
    private boolean monthAllRank;//代表活动期间按月统计的礼物榜单
    private boolean hourSpecificRank;//代表活动期间按小时统计的特殊礼物榜单
    private boolean daySpecificRank;//代表活动期间按天统计的特殊礼物榜单
    private boolean weekSpecificRank;//代表活动期间按周统计的特殊礼物榜单
    private boolean monthSpecificRank;//代表活动期间按月统计的特殊礼物榜单
    private int flag;//0:开播有记录即统计，1:主播名单固定方式，前端报名的方式会使用flag=0,接口过滤报名的主播，hostpool:{project},2:分组报名方式，hostpool:{project}:groups用来存组表示，hostpool:{project}:group:{group}只有此处用-分隔，兼容业务
//    private Set<String> qids = new HashSet<>();
    //    private Map<String, Set<String>> groupAnchorsMap = new HashMap<>();//每个组对应的主播列表
//    private Map<String, String> anchor2GroupMap = new HashMap<>();//主播对应的组


    public boolean isSpecificExtraAdd() {
        return specificExtraAdd;
    }

    public void setSpecificExtraAdd(boolean specificExtraAdd) {
        this.specificExtraAdd = specificExtraAdd;
    }

    public double getSpecificExtraValue() {
        return specificExtraValue;
    }

    public void setSpecificExtraValue(double specificExtraValue) {
        this.specificExtraValue = specificExtraValue;
    }

    public boolean isWeekPopularRank() {
        return weekPopularRank;
    }

    public void setWeekPopularRank(boolean weekPopularRank) {
        this.weekPopularRank = weekPopularRank;
    }

    public boolean isMonthPopularRank() {
        return monthPopularRank;
    }

    public void setMonthPopularRank(boolean monthPopularRank) {
        this.monthPopularRank = monthPopularRank;
    }

    public boolean isPopularRank() {
        return popularRank;
    }

    public void setPopularRank(boolean popularRank) {
        this.popularRank = popularRank;
    }

    public boolean isMonthAllRank() {
        return monthAllRank;
    }

    public void setMonthAllRank(boolean monthAllRank) {
        this.monthAllRank = monthAllRank;
    }

    public boolean isMonthSpecificRank() {
        return monthSpecificRank;
    }

    public void setMonthSpecificRank(boolean monthSpecificRank) {
        this.monthSpecificRank = monthSpecificRank;
    }

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
                popularRank == that.popularRank &&
                weekPopularRank == that.weekPopularRank &&
                monthPopularRank == that.monthPopularRank &&
                allRank == that.allRank &&
                specificRank == that.specificRank &&
                hourAllRank == that.hourAllRank &&
                dayAllRank == that.dayAllRank &&
                weekAllRank == that.weekAllRank &&
                hourSpecificRank == that.hourSpecificRank &&
                daySpecificRank == that.daySpecificRank &&
                weekSpecificRank == that.weekSpecificRank &&
                flag == that.flag &&
                specificExtraAdd == that.specificExtraAdd &&
                specificExtraValue == that.specificExtraValue &&
                Objects.equals(project, that.project) &&
                Objects.equals(cates, that.cates) &&
                Objects.equals(giftIds, that.giftIds);
    }

    @Override
    public int hashCode() {
        return Objects.hash(project);
    }

    @Override
    public String toString() {
        return "RankProject{" +
                "project='" + project + '\'' +
                ", startTimeU=" + startTimeU +
                ", endTimeU=" + endTimeU +
                ", cates=" + cates +
                ", giftIds=" + giftIds +
                ", popularRank=" + popularRank +
                ", weekPopularRank=" + weekPopularRank +
                ", monthPopularRank=" + monthPopularRank +
                ", allRank=" + allRank +
                ", specificRank=" + specificRank +
                ", hourAllRank=" + hourAllRank +
                ", dayAllRank=" + dayAllRank +
                ", weekAllRank=" + weekAllRank +
                ", monthAllRank=" + monthAllRank +
                ", hourSpecificRank=" + hourSpecificRank +
                ", daySpecificRank=" + daySpecificRank +
                ", weekSpecificRank=" + weekSpecificRank +
                ", monthSpecificRank=" + monthSpecificRank +
                ", specificExtraAdd=" + specificExtraAdd +
                ", specificExtraValue=" + specificExtraValue +
                ", flag=" + flag +
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
        boolean equals = rank1.equals(rank2);
        System.out.println(equals);
    }
}
