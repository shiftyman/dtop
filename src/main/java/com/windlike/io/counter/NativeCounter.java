package com.windlike.io.counter;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashLongSet;
import com.windlike.io.Constants;
import com.windlike.io.reader.ActFileReader2;
import com.windlike.io.reader.LikeGoodsFileReader2;
import com.windlike.io.util.TopKComputer;
import com.windlike.io.vo.ActivityVo;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by windlike.xu on 2018/3/4.
 */
public class NativeCounter {


    public HashLongObjMap<HashLongSet> getBrandMap() {
        return brandMap;
    }

    public List<ActivityVo> getActivityVos() {
        return activityVos;
    }

    private List<ActivityVo> activityVos;

    private HashLongObjMap<HashLongSet> brandMap;


    public NativeCounter() {
    }

    public void count() throws InterruptedException {
        //1.加载活动信息
        long t1 = System.currentTimeMillis();
        long start = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();


        brandMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);

        ActFileReader2 actFileReader = new ActFileReader2(Constants.ACT_DATA_FILE, brandMap);
        activityVos = actFileReader.call();
        actFileReader = null;

        long t2 = System.currentTimeMillis();

//        System.gc();//// TODO: 2018/3/6
//        Thread.sleep(2000L);
        long mid = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("time:" + (t2-t1) +  ",mem:" + (mid - start));

        //2.加载单个收藏数据，并做样本计算
        File likeGoodsPath = new File(Constants.LIKE_GODDS_DATA_FILE_PATH);
        String[] fileNames = likeGoodsPath.list();
        LikeGoodsFileReader2 likeGoodsFileReader = new LikeGoodsFileReader2(Constants.LIKE_GODDS_DATA_FILE_PATH + fileNames[0], activityVos, brandMap);
        try {
            likeGoodsFileReader.call();
            likeGoodsFileReader = null;
        } catch (Exception e) {
            e.printStackTrace();
        }

        long t3 = System.currentTimeMillis();
//        System.gc();//// TODO: 2018/3/6
//        Thread.sleep(2000L);
        long end = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("time:" + (t3-t2) + ",mem:" + (end - start));
        System.out.println("actmap:" + activityVos.size() + ",brandmap:" + brandMap.size() + ",brandlinksum:" + TopKComputer.printBrandActLinkSum(brandMap));

        //3.样本之后压缩有效集合
        activityVos = TopKComputer.compressActivityMap(activityVos);
        brandMap = TopKComputer.compressBrandMap(brandMap, activityVos);

        long t4 = System.currentTimeMillis();
//        System.gc();//// TODO: 2018/3/6
//        Thread.sleep(2000L);
        long end2 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("time:" + (t4-t3) + ",mem:" + (end2 - start));
        System.out.println("newactmap:" + activityVos.size() + ",newbrandmap:" + brandMap.size() + ",brandlinksum:" + TopKComputer.printBrandActLinkSum(brandMap));

        //4.读取所有收藏数据并计算
        ExecutorService pool = Executors.newFixedThreadPool(Constants.DEFAULT_LIKE_GOOD_THREAD_NUM);
        List<Future> futures = new ArrayList<>(fileNames.length - 1);
        for (int i = 1; i < fileNames.length; i++){
            futures.add(pool.submit(new LikeGoodsFileReader2(Constants.LIKE_GODDS_DATA_FILE_PATH + fileNames[i], activityVos, brandMap)));
        }
        for(Future future : futures){
            try {
                future.get();//wait all completed
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        long t5 = System.currentTimeMillis();
//        System.gc();//// TODO: 2018/3/6
//        Thread.sleep(2000L);
        long end3 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("time:" + (t5-t4) + ",mem:" + (end3 - start));
//    }

//        TopKComputer.collectInfoAndPrint(activityVos);//print
//        Thread.sleep(5000L);
    }

}
