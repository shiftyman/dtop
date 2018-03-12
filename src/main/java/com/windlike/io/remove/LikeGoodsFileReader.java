//package com.windlike.io.reader;
//
//import com.koloboke.collect.map.hash.HashLongObjMap;
//import com.koloboke.collect.set.hash.HashLongSet;
//import com.windlike.io.Constants;
//import com.windlike.io.vo.Platforms;
//import com.windlike.io.util.InvincibleConvertUtil;
//import com.windlike.io.vo.ActivityVo;
//
//import java.io.BufferedInputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.util.concurrent.Callable;
//
///**
// * Created by windlike.xu on 2018/3/2.
// */
//public class LikeGoodsFileReader implements Callable<Void>{
//
//    private String orignFileFullName;
//
//    private HashLongObjMap<ActivityVo> activityMap;
//
//    private HashLongObjMap<HashLongSet> brandMap;//val为'MMddHHmmssSSS(actname)|MMdd(endtime)'的list
//
//    private int row = 0;//行数
//
//    public LikeGoodsFileReader(String orignFileFullName, HashLongObjMap<ActivityVo> activityMap, HashLongObjMap<HashLongSet> brandMap) {
//        this.orignFileFullName = orignFileFullName;
//        this.activityMap = activityMap;
//        this.brandMap = brandMap;//brand和act的数量相当
//    }
//
//    public Void call() throws Exception {
//        long t1 = System.currentTimeMillis();
//
//        try(BufferedInputStream bis = new BufferedInputStream(new FileInputStream(new File(orignFileFullName)))){
//            int segmentType = 0;
//            int length = 0;
//            byte[] fileSegmentCache = new byte[200];
//            int userId = 0;
//            int addTime = 0;//MMddHHmmss
//            byte platform = 0;
//            long brandId = 0;
//            boolean eof = false;
//            while(!eof){
//                switch (segmentType){
//                    case 0://userId
//                        length = getSegment(bis, fileSegmentCache);
//                        if(length <= 0){
//                            eof = true;
//                        }
//
//                        userId = InvincibleConvertUtil.byteArrayToInt(fileSegmentCache, 0, length);
//                        break;
//                    case 1://addtime
//                        char c = (char) bis.read();
//                        c = (char) bis.read();
//                        c = (char) bis.read();
//                        if(bis.read() != Constants.SEVEN_ASCII){
//                            //无用数据，弃用
//                            getLine(bis, fileSegmentCache);
//                            segmentType = 3;
//                        }else{
//                            c = (char) bis.read();
//                            getStubSegment(bis, fileSegmentCache, 0, 2);
//                            bis.read();
//                            getStubSegment(bis, fileSegmentCache, 2, 2);
//                            bis.read();
//                            getStubSegment(bis, fileSegmentCache, 4, 2);
//                            bis.read();
//                            getStubSegment(bis, fileSegmentCache, 6, 2);
//                            bis.read();
//                            getStubSegment(bis, fileSegmentCache, 8, 2);
//                            bis.read();
//                            addTime = InvincibleConvertUtil.byteArrayToInt(fileSegmentCache, 0, 10);
//                            //跳过goodId
//                            getSegment(bis, fileSegmentCache);
//                        }
//                        break;
//                    case 2://platform
//                        bis.read();
//                        platform = Platforms.shortNameToIndex((char) bis.read());
//                        //skip to next
//                        getStubSegment(bis, fileSegmentCache, 0, Platforms.indexToLeftCharNum(platform));
//                        break;
//                    case 3://brandId
//                        length = getSegment(bis, fileSegmentCache);
//                        brandId = InvincibleConvertUtil.byteArrayToInt(fileSegmentCache, 0, length);
//                        //save the data
//                        long brandKey = (brandId << 2) + platform;
//                        HashLongSet brandActs = brandMap.get(brandKey);
//                        if(brandActs != null){
//                            int finalAddTime = addTime;
//                            byte finalPlatform = platform;
//                            int finalUserId = userId;
//                            brandActs.cursor().forEachForward((actTime)-> {//MMddHHmmssSSS(actname)|MMdd(endtime)
//                                int startTime = (int) (actTime / 10000000000000L) * 1000000 + 100000;
//                                long tmp = actTime / 10000;
//                                int endTime = (int) (actTime - tmp * 10000) * 1000000 + 95959;
//                                if(finalAddTime >= startTime && finalAddTime <= endTime){
//                                    long actPlatform = ((tmp + 20170000000000000L) << 2) + finalPlatform;
//                                    ActivityVo act = activityMap.get(actPlatform);
//                                    act.addUser(finalUserId, 1);
//                                }
//                            });
//                        }
//
//                        //skip to next line
////                    getStubSegment(bis, fileSegmentCache, 0, 11);//收藏数据bug，最后有空格 todo
//                        getLine(bis, fileSegmentCache);
//                        break;
//                }
//
//                if(segmentType == 3){//一行结束，重置
//                    segmentType = 0;
//
//                    row++;//// TODO: 2018/3/1
//                }else{
//                    segmentType++;
//                }
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }
//
//        activityMap = null;
//        brandMap = null;
//
//        System.out.println("收藏行数:" + row + ",time:" + (System.currentTimeMillis() - t1));
//        return null;
//    }
//
//    private int getSegment(BufferedInputStream bis, byte[] fileSegmentCache) {
//        int i = 0;
//        try{
//            for(; i < fileSegmentCache.length; i++){
//                byte b = (byte) bis.read();
//                if(b == Constants.SEGMENT_SEPARATE_SYMBOL || b == Constants.NEW_LINE_CHAR_ASCII || b == Constants.EOF_FILE){
//                    break;
//                }
//                fileSegmentCache[i] = b;
//            }
//        }catch (Exception e){
//            //已经是结尾
//        }
//        return i;
//    }
//
//    private int getLine(BufferedInputStream bis, byte[] fileSegmentCache){
//        int i = 0;
//        try{
//            for(; i < fileSegmentCache.length; i++){
//                byte b = (byte) bis.read();
//                if(b == Constants.NEW_LINE_CHAR_ASCII || b == Constants.EOF_FILE){
//                    break;
//                }
//                fileSegmentCache[i] = b;
//            }
//        }catch (Exception e){
//            //已经是结尾
//        }
//        return i;
//    }
//
//    private int getStubSegment(BufferedInputStream bis, byte[] fileSegmentCache, int start, int length) throws IOException {
//        return bis.read(fileSegmentCache, start, length);
//    }
//
//    public static void main(String[] args) {
//        long actTime = 10251250201231106L;
//        int startTime = (int) (actTime / 10000000000000L) * 1000000 + 100000;
//        long tmp = actTime / 10000;
//        int endTime = (int) (actTime - tmp * 10000) * 1000000 + 95959;
//        long actPlatformpp = ((tmp + 20170000000000000L) << 2);
//        System.out.println(startTime + "," + endTime + "," + actPlatformpp);
//
//        System.out.println(80680872686407358L & 3);
//    }
//}
