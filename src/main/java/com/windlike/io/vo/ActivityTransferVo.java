package com.windlike.io.vo;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.windlike.io.Constants;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class ActivityTransferVo{

    private long actPlatfrom;//yyyyMMddHHmmssSSS|pFlag pFlag占后两位

    private int allNum;

//    private IntArrayList userNumList;
    int[] userNumList;

    public int[] getUserNumList() {
        return userNumList;
    }

    public ActivityTransferVo(long actPlatfrom, int allNum, int userNum) {
        this.actPlatfrom = actPlatfrom;
        this.allNum = allNum;
//        this.userNumList = new IntArrayList(userNum * 2);
        userNumList = new int[userNum * 2];
    }

//    public IntArrayList getUserNumList() {
//        return userNumList;
//    }


    public long getActPlatfrom() {
        return actPlatfrom;
    }

    public int getAllNum() {
        return allNum;
    }
}
