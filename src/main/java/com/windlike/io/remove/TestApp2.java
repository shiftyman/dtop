//package com.windlike.io;
//
//import com.koloboke.collect.map.hash.HashLongObjMap;
//import com.koloboke.collect.map.hash.HashLongObjMaps;
//import com.koloboke.collect.set.hash.HashLongSet;
//import com.koloboke.collect.set.hash.HashLongSets;
//import com.windlike.io.reader.ActFileReader;
//import com.windlike.io.reader.ActFileReader2;
//import com.windlike.io.reader.LikeGoodsFileReader;
//import com.windlike.io.reader.LikeGoodsFileReader2;
//import com.windlike.io.util.TopKComputer;
//import com.windlike.io.vo.ActivityVo;
//
//import java.io.File;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//
///**
// * Created by windlike.xu on 2018/3/3.
// */
//public class TestApp2 {
//
//    public static void main2(String[] args) {
//        long actPlatform = 10251020001230526L / 10000 + 20170000000000000L;
//        System.out.println(actPlatform);
//    }
//
//    public static void main(String[] args) throws InterruptedException {
//        int threadNum = 8;
//
//        String fileName = null;
//        if(args.length > 0){
//            threadNum = Integer.parseInt(args[0]);
//            if(args.length > 1){
//                fileName = args[1];
//            }
//        }
//
//        //1.加载活动信息
//        long t1 = System.currentTimeMillis();
//        long start = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//        HashLongObjMap<ActivityVo> activityMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);
//        HashLongObjMap<HashLongSet> brandMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);
//
//        ActFileReader actFileReader = new ActFileReader(Constants.ACT_DATA_FILE, activityMap, brandMap);
//        actFileReader.read();
//        actFileReader = null;
//
//        long t2 = System.currentTimeMillis();
//
//        System.gc();
//        Thread.sleep(2000L);
//        long mid = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//        System.out.println("time:" + (t2-t1) +  ",mem:" + (mid - start));
//
//        //2.加载单个收藏数据，并做样本计算
//        File likeGoodsPath = new File(Constants.LIKE_GODDS_DATA_FILE_PATH);
//        String[] fileNames = likeGoodsPath.list();
//        if(fileName == null){
//            fileName = fileNames[0];// todo delete
//        }
//        LikeGoodsFileReader likeGoodsFileReader = new LikeGoodsFileReader(Constants.LIKE_GODDS_DATA_FILE_PATH + fileName, activityMap, brandMap);
//        try {
//            likeGoodsFileReader.call();
//            likeGoodsFileReader = null;
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        long t3 = System.currentTimeMillis();
//        System.gc();
//        Thread.sleep(2000L);
//        long end = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//        System.out.println("time:" + (t3-t2) + ",mem:" + (end - start));
//        System.out.println("actmap:" + activityMap.size() + ",brandmap:" + brandMap.size() + ",brandlinksum:" + TopKComputer.printBrandActLinkSum(brandMap));
//
//        //3.样本之后压缩有效集合
//        activityMap = TopKComputer.compressActivityMap(activityMap);
//        brandMap = TopKComputer.compressBrandMap(brandMap, activityMap);
//
//        long t4 = System.currentTimeMillis();
//        System.gc();
//        Thread.sleep(2000L);
//        long end2 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//        System.out.println("time:" + (t4-t3) + ",mem:" + (end2 - start));
//        System.out.println("newactmap:" + activityMap.size() + ",newbrandmap:" + brandMap.size() + ",brandlinksum:" + TopKComputer.printBrandActLinkSum(brandMap));
//
//        //4.读取所有收藏数据并计算
//        if(args.length <= 1){//// TODO: 2018/3/3 测试的话跳过
//            ExecutorService pool = Executors.newFixedThreadPool(threadNum);
//            List<Future> futures = new ArrayList<>(fileNames.length - 1);
//            for (int i = 1; i < fileNames.length; i++){
//                futures.add(pool.submit(new LikeGoodsFileReader(Constants.LIKE_GODDS_DATA_FILE_PATH + fileNames[i], activityMap, brandMap)));
//            }
//            for(Future future : futures){
//                try {
//                    future.get();//wait all completed
//                } catch (ExecutionException e) {
//                    e.printStackTrace();
//                }
//            }
//
//            long t5 = System.currentTimeMillis();
//            System.gc();
//            Thread.sleep(2000L);
//            long end3 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
//
//            System.out.println("time:" + (t5-t4) + ",mem:" + (end3 - start));
//        }
//
//        TopKComputer.collectInfoAndPrint(activityMap);//print
//        Thread.sleep(5000L);
//    }
//
//
//}
