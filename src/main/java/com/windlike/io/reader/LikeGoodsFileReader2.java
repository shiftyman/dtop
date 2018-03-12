package com.windlike.io.reader;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.set.hash.HashLongSet;
import com.windlike.io.Constants;
import com.windlike.io.Platforms;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.vo.ActivityVo;

import java.io.*;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Created by windlike.xu on 2018/3/2.
 */
public class LikeGoodsFileReader2 implements Callable<Void>{

    private String orignFileFullName;

    private HashLongObjMap<ActivityVo> activityMap;

    private HashLongObjMap<HashLongSet> brandMap;//val为'MMddHHmmssSSS(actname)|MMdd(endtime)'的list

    private int row = 0;//行数

    public LikeGoodsFileReader2(String orignFileFullName, HashLongObjMap<ActivityVo> activityMap, HashLongObjMap<HashLongSet> brandMap) {
        this.orignFileFullName = orignFileFullName;
        this.activityMap = activityMap;
        this.brandMap = brandMap;//brand和act的数量相当
    }



    public Void call() throws Exception {
        try(BufferedReader br = new BufferedReader(new FileReader(new File(orignFileFullName)))){
            String oneLine;
            while((oneLine = br.readLine()) != null){
                row++;

                List<String> pieces = InvincibleConvertUtil.split(oneLine, ',', 6);
                String addTimeStr = pieces.get(1);
                if(addTimeStr.charAt(3) == Constants.SEVEN_ASCII){//先看年份吧
                    long brandId = InvincibleConvertUtil.stringToLong(pieces.get(4));
                    int platform = Platforms.shortNameToIndex(pieces.get(3).charAt(1));
                    long brandKey = (brandId << 2) + platform;

                    HashLongSet brandActs = brandMap.get(brandKey);
                    if(brandActs != null){
                        int addTime = InvincibleConvertUtil.dateToMMddhhmmss(addTimeStr);
                        int userId = InvincibleConvertUtil.stringToInt(pieces.get(0));
                        brandActs.cursor().forEachForward((actTime)-> {//MMddHHmmssSSS(actname)|MMdd(endtime)
                            int startTime = (int) (actTime / 10000000000000L) * 1000000 + 100000;
                            long tmp = actTime / 10000;
                            int endTime = (int) (actTime - tmp * 10000) * 1000000 + 95959;
                            if(addTime >= startTime && addTime <= endTime){
                                long actPlatform = ((tmp + 20170000000000000L) << 2) + platform;
                                ActivityVo act = activityMap.get(actPlatform);
                                act.addUser(userId, 1);
                            }
                        });
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        activityMap = null;
        brandMap = null;

        System.out.println("收藏行数:" + row);
        return null;
    }


//    public static void main(String[] args) {
//        long t1 = System.currentTimeMillis();
//        for(long i = 0; i < 1000000L;i ++){
//            dateToMMddhhmmss("2017-10-10 16:00:01");
//        }
//        long t2 = System.currentTimeMillis();
//
//        System.out.println(t2-t1);
//    }
}
