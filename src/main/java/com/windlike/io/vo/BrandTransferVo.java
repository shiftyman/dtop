package com.windlike.io.vo;

import java.util.Arrays;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class BrandTransferVo {

    private String brandName;

    private long brandId;

    private byte platform;

    private long startTime;

    private long endTime;

    private int allNum;

    private long[] userIdAndNum;

    private short userIndex = 0;

    public BrandTransferVo(String brandName, long brandId, byte platform, long startTime, long endTime, int allNum
        , int userNum) {
        this.brandName = brandName;
        this.brandId = brandId;
        this.platform = platform;
        this.startTime = startTime;
        this.endTime = endTime;
        this.allNum = allNum;
        userIdAndNum = new long[userNum << 1];
    }

    public void addUser(long user, long num){
        userIdAndNum[userIndex] = user;
        userIdAndNum[userIndex + 1] = num;
        userIndex += 2;
    }

    @Override
    public String toString() {
        return "Brand{" +
                "brandName='" + brandName + '\'' +
                ", brandId=" + brandId +
                ", platform=" + platform +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", allNum=" + allNum +
                ", userIdAndNum=" + Arrays.toString(userIdAndNum) +
                '}';
    }

    public String getBrandName() {
        return brandName;
    }

    public long getBrandId() {
        return brandId;
    }

    public byte getPlatform() {
        return platform;
    }

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public int getAllNum() {
        return allNum;
    }

    public long[] getUserIdAndNum() {
        return userIdAndNum;
    }


}
