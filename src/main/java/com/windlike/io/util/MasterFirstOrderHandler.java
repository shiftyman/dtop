package com.windlike.io.util;

import com.sun.xml.internal.ws.model.RuntimeModelerException;
import com.windlike.io.Constants;
import com.windlike.io.vo.ActivityVo;

import java.util.concurrent.CountDownLatch;

/**
 * Created by windlike.xu on 2018/3/6.
 */
public class MasterFirstOrderHandler {

    private int[] actAbstracts;
    private ActivityVo[] topK;
    private CountDownLatch countDownLatch;//首单完成度

    private static MasterFirstOrderHandler pseudoSingleton;

    private MasterFirstOrderHandler(ActivityVo[] topK, int[] actAbstracts){
        this.actAbstracts = actAbstracts;
        this.topK = topK;
        this.countDownLatch = new CountDownLatch(Constants.FILE_SPLIT_NUM);
    }

    public static void initSingleton(ActivityVo[] topK, int[] actAbstracts){
        pseudoSingleton = new MasterFirstOrderHandler(topK, actAbstracts);
    }

    public static MasterFirstOrderHandler getInstance(){
        if(pseudoSingleton == null){
            throw new RuntimeException("请先调用initSingleton进行初始化");
        }
        return pseudoSingleton;
    }

    public void waitAllComplete(){
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void finishOne(){
        countDownLatch.countDown();
    }

    public void processFirstOrder(int userId, int firstOrderTime){
        for (int i = 0; i < actAbstracts.length; i++){
            int endTime = actAbstracts[i];
//                int tx  = abs / 1000000 * 1000000;
//                int startTime = tx + 100000;
//                int endTime = (abs - tx) / 100 * 1000000 + 95959;
            if(firstOrderTime <= endTime){//老客，削减
//                if(topK[i].containsUser(userId)){
                    topK[i].findAnOldUser(userId);
//                }
            }
        }
    }
}
