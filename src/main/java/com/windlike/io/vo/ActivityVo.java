package com.windlike.io.vo;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.windlike.io.Constants;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntListIterator;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class ActivityVo implements Comparable<ActivityVo>, Cloneable{

    private long actPlatfrom;//yyyyMMddHHmmssSSS|pFlag pFlag占后两位

    private int startTime;//MMddHHmmss

    private int endTime;//MMddHHmmss

    private int allNum;

    private HashIntIntMap userNumMap;

    private byte mergeNum = 1;

    public boolean containsUser(int userId){
        return userNumMap.containsKey(userId);
    }

    public void findAnOldUser(int userId){
        int num = userNumMap.get(userId);
        if(num > 0){//存在
            minusNewUserAllTimes.addAndGet(num);
            minusNewUserNum.incrementAndGet();
        }
//        minusNewUserAllTimes += userNumMap.get(userId);
//        minusNewUserNum ++;
    }

    public AtomicInteger getMinusNewUserAllTimes() {
        return minusNewUserAllTimes;
    }

    public AtomicInteger getMinusNewUserNum() {
        return minusNewUserNum;
    }

    private AtomicInteger minusNewUserAllTimes = new AtomicInteger(0);//新客收藏人数
//
    private AtomicInteger minusNewUserNum = new AtomicInteger(0);//新客户收藏人次

//    private int minusNewUserAllTimes = 0;//新客收藏人数
//
//    private int minusNewUserNum = 0;//新客户收藏人次

    public ActivityVo() {
        userNumMap = HashIntIntMaps.newUpdatableMap(Constants.SUPPOSE_ACTIVITY_UNIQUE_USER_NUM);
    }


    /**
     *
     * @param actPlatfrom
     * @param startTime
     * @param endTime
     * @param allNum
     * @param userNum
     */
    public ActivityVo(long actPlatfrom, int startTime, int endTime, int allNum, int userNum) {
        this.actPlatfrom = actPlatfrom;
        this.startTime = startTime;
        this.endTime = endTime;
        this.allNum = allNum;
        this.userNumMap = HashIntIntMaps.newUpdatableMap(userNum);
    }

    public void merge(ActivityVo vo){
        this.allNum += vo.getAllNum();
        vo.getUserNumMap().cursor().forEachForward((k, v)->this.userNumMap.addValue(k, v));
        mergeNum ++;
    }

    public void merge(ActivityTransferVo vo){
        this.allNum += vo.getAllNum();
        IntArrayList intList = vo.getUserNumList();
        IntListIterator iterator = intList.iterator();
        while (iterator.hasNext()){
            this.userNumMap.addValue(iterator.nextInt(),iterator.nextInt());
        }
        mergeNum ++;
    }


    public synchronized void addUser(int userId, int num){//// TODO: 2018/3/7 synchronized
        userNumMap.addValue(userId, num);
        allNum += num;
    }



    @Override
    public String toString() {
        return "ActivityVo{" +
                "actPlatfrom=" + actPlatfrom +
//                ", startTime=" + startTime +
//                ", endTime=" + endTime +
                ", allNum=" + allNum +
                ", userNum=" + userNumMap.size() +
                ", newUserAllTimes=" + (allNum - minusNewUserAllTimes.get()) +
                ", newUserNum=" + (userNumMap.size() - minusNewUserNum.get()) +
//                ", newUserAllTimes=" + (allNum - minusNewUserAllTimes) +
//                ", newUserNum=" + (userNumMap.size() - minusNewUserNum) +
                '}';
    }

    public void setActPlatfrom(long actPlatfrom) {
        this.actPlatfrom = actPlatfrom;
    }

    public void setStartTime(int startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(int endTime) {
        this.endTime = endTime;
    }

    public void setUserNumMap(HashIntIntMap userNumMap) {
        this.userNumMap = userNumMap;
    }

    public void setAllNum(int allNum) {
        this.allNum = allNum;
    }

    public void setMergeNum(byte mergeNum) {
        this.mergeNum = mergeNum;
    }

    public long getActPlatfrom() {
        return actPlatfrom;
    }

    public int getStartTime() {
        return startTime;
    }

    public int getEndTime() {
        return endTime;
    }

    public HashIntIntMap getUserNumMap() {
        return userNumMap;
    }

    public int getAllNum() {
        return allNum;
    }

//    public boolean isHasSent() {
//        return hasSent;
//    }

    public byte getMergeNum() {
        return mergeNum;
    }

    public int compareByAllNumAsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return -1;
        }else if(this.allNum < o.allNum){
            return 1;
        }else {
            return 0;
        }
    }

    public int compareByAllNumDsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return 1;
        }else if(this.allNum < o.allNum){
            return -1;
        }else {
            return 0;
        }
    }

    public int compareFullAsc(ActivityVo o) {
        if(this.allNum > o.allNum){
            return -1;
        }else if(this.allNum < o.allNum){
            return 1;
        }else {
            if(this.userNumMap.size() > o.userNumMap.size()){
                return -1;
            }else if(this.userNumMap.size() < o.userNumMap.size()){
                return 1;
            }else{
                if(this.actPlatfrom < o.actPlatfrom){
                    return -1;
                }else{
                    return 1;
                }
            }
        }
    }

    @Override
    public int compareTo(ActivityVo o) {
        return compareByAllNumDsc(o);
    }

    public static void main(String[] args) {

//        HashLongLongMap longLongMap = HashLongLongMaps.newUpdatableMap();
//        longLongMap.addValue(1, 3);
//        longLongMap.addValue(2, 4);
//        System.out.println(longLongMap.get(1));
//        longLongMap.cursor().forEachForward((k, v)-> System.out.println(k + "," + v));

        byte s1 = (byte) (0xf0 + 1);
        byte s2 = (byte) (0xf0 + 2);
        System.out.println(s1&s2);
        System.out.println((byte)0xf0);

        System.out.println(Constants.NOT_SENT_FLAG);
        System.out.println(Constants.SENT_FLAG);
    }

    //    }
//        return mflag;

}
