package com.windlike.io.reader;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.set.hash.HashLongSet;
import com.windlike.io.Constants;
import com.windlike.io.vo.Platforms;
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

    private List<ActivityVo> activityList;

    private HashLongObjMap<HashLongSet> brandMap;//val为'MMddHHmmssSSS(actname)|MMdd(endtime)'的list

    private int row = 0;//行数

    public LikeGoodsFileReader2(String orignFileFullName, List<ActivityVo> activityList, HashLongObjMap<HashLongSet> brandMap) {
        this.orignFileFullName = orignFileFullName;
        this.activityList = activityList;
        this.brandMap = brandMap;//brand和act的数量相当
    }

    public Void call() throws Exception {
        long t1 = System.currentTimeMillis();

        try(BufferedReader br = new BufferedReader(new FileReader(new File(orignFileFullName)), Constants.FILE_BUFFER_SIZE)){
            String oneLine;
            while((oneLine = br.readLine()) != null){
                row++;

//                List<String> pieces = InvincibleConvertUtil.split(oneLine, ',', 6);
                String[] pieces = InvincibleConvertUtil.split(oneLine, ',', 6);
                String addTimeStr = pieces[1];
                if(addTimeStr.charAt(3) == Constants.SEVEN_ASCII){//先看年份吧
                    long brandId = InvincibleConvertUtil.stringToLong(pieces[4]);
                    int platform = Platforms.shortNameToIndex(pieces[3].charAt(1));
                    long brandKey = (brandId << 2) + platform;

                    HashLongSet brandActs = brandMap.get(brandKey);
                    if(brandActs != null){
                        int addTime = InvincibleConvertUtil.dateToMMddhhmmss(addTimeStr);
                        int userId = InvincibleConvertUtil.stringToInt(pieces[0]);
                        brandActs.cursor().forEachForward((x)-> {//MMddHHmmssSSS(actname)|MMdd(endtime)
//                            int startTime = (int) (actTime / 10000000000000L) * 1000000 + 100000;
//                            long tmp = actTime / 10000;
//                            int endTime = (int) (actTime - tmp * 10000) * 1000000 + 95959;
//                            if(addTime >= startTime && addTime <= endTime){
//                                long actPlatform = ((tmp + 20170000000000000L) << 2) + platform;
//                                ActivityVo act = activityMap.get(actPlatform);
//                                act.addUser(userId, 1);
//                            }
                            int index = (int) (x & 0xffff);
                            int endTime = (int) (((x-index) >>> 16) & 0xffff) * 1000000 + 95959;
                            int startTime = (int) (x >>> 32) * 1000000 + 100000;
                            if(addTime >= startTime && addTime <= endTime){
                                ActivityVo act = activityList.get(index);
                                act.addUser(userId, 1);
                            }
                        });
                    }
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

        activityList = null;
        brandMap = null;

        System.out.println("收藏行数:" + row + ", time:" + (System.currentTimeMillis() - t1));
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
