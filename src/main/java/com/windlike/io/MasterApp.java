package com.windlike.io;

import com.koloboke.collect.impl.hash.Hash;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import com.windlike.io.bio.Server;
import com.windlike.io.counter.NativeCounter;
import com.windlike.io.reader.*;
import com.windlike.io.util.MasterFirstOrderHandler;
import com.windlike.io.util.TopKComputer;
import com.windlike.io.vo.ActivityTransferVo;
import com.windlike.io.vo.ActivityVo;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by windlike.xu on 2018/3/3.
 */
public class MasterApp {

    public static LinkedBlockingQueue<ActivityTransferVo[]> remoteTopN = new LinkedBlockingQueue<>(2<<1);

    public static void main(String[] args) throws InterruptedException {
        long t1 = System.currentTimeMillis();


        System.out.println("App run in master mode");



        Server server = new Server(Constants.SERVER_LISTENING_PORT);
        server.start();

        sendStartToSlave();

        //native counting start and get native topN result
        NativeCounter nativeCounter = null;
        if(args.length > 0){
            nativeCounter = new NativeCounter(Integer.parseInt(args[0]));
        }else{
            nativeCounter = new NativeCounter();
        }
        nativeCounter.count();
        HashLongObjMap<ActivityVo> activityMap = nativeCounter.getActivityMap();
//        HashLongObjMap<HashLongSet> brandMap = nativeCounter.getBrandMap();
        nativeCounter = null;

        //get remote topN result
        long t11 = System.currentTimeMillis();
        mergeToMap(activityMap, remoteTopN.take());
        mergeToMap(activityMap, remoteTopN.take());

        long t2 = System.currentTimeMillis();

        System.out.println("mergeToMap两个完成，time:" + (t2 - t11));

        //cal top k
        ActivityVo[] topK = TopKComputer.computeTopKAfterMerge(activityMap, Constants.K);
        activityMap = null;

        //****首单计算*****/
        int firstOrderTaskNum = 8;
        if(args.length > 1){
            firstOrderTaskNum = Integer.parseInt(args[1]);
        }
        ExecutorService pool = Executors.newFixedThreadPool(firstOrderTaskNum);

        //get user first time
//        HashIntSet userIdSet = getUiqueUserSet(topK);
        //初始化userIdBitSet
        BitSet userIdBitSet = getUiqueUserBitSet(topK);//// TODO: 2018/3/7

        //generate actAbstracts
        int[] actAbstracts = generateActAbstract(topK);
        MasterFirstOrderHandler.initSingleton(topK, actAbstracts);//初始化首单处理器

        File firstOrderPath = new File(Constants.FIRST_ORDER_FILE_PATH);
        String[] firstOrderFileNames = firstOrderPath.list();
//        AtomicInteger counter = new AtomicInteger(userIdSet.size());
        for (int i = 0; i < firstOrderFileNames.length; i++){
            pool.submit(new FirstOrderFileReaderByBuffer(Constants.FIRST_ORDER_FILE_PATH + firstOrderFileNames[i],
                    userIdBitSet, topK, actAbstracts));
        }

        //发起首单查询到slave
        sendUserOrderTimeQuery(userIdBitSet);

//        for(Future future : futures){
//            try {
//                future.get();//wait all completed
//            } catch (ExecutionException e) {
//                e.printStackTrace();
//            }
//        }

        //等待所有首单文件处理结束
        MasterFirstOrderHandler.getInstance().waitAllComplete();

        long t7 = System.currentTimeMillis();
//        long end4 = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

        System.out.println("总时间time:" + (t7-t1) + ",收藏数据耗时：" + (t2-t1) + ",首单耗时:"
                + (t7-t2));

        printTopKResult(topK);//print
//        Thread.sleep(5000L);
    }

    public static void printTopKResult(ActivityVo[] activityVos){
        long userIdNotUnique = 0;
        for (ActivityVo activityVo : activityVos){
//            System.out.println(activityVo);
            long actPlatform = activityVo.getActPlatfrom();
            HashIntIntMap userNumMap = activityVo.getUserNumMap();
            long actName = actPlatform >> 2;
            String platform = Platforms.indexToFullName((byte) (actPlatform & 3));
            System.out.println(actName+","+platform+","+activityVo.getAllNum()+","
                +userNumMap.size()+","+(activityVo.getAllNum() - activityVo.getMinusNewUserAllTimes().get()) +
                    "," + (userNumMap.size() - activityVo.getMinusNewUserNum().get()));
            userIdNotUnique += activityVo.getUserNumMap().size();
        }

//        System.out.println("\n unique userId size:" + userIdSet.size());
        System.out.println("\n not unique userId size:" + userIdNotUnique);

    }

    public static int[] generateActAbstract(ActivityVo[] activityVos){
        int[] result = new int[activityVos.length];
        for (int i = 0; i < activityVos.length; i++) {
            ActivityVo vo = activityVos[i];
            result[i] = vo.getEndTime();//vo.getStartTime() / 1000000 * 10000 + vo.getEndTime() / 1000000;
        }

        return result;
    }

//    public static HashIntSet getUiqueUserSet(ActivityVo[] activityVos){
//        HashIntSet userIdSet = HashIntSets.newUpdatableSet(Constants.SUPPOSE_TOPK_USERIDS);
//        for(ActivityVo activityVo : activityVos){
//            HashIntIntMap userNumMap = activityVo.getUserNumMap();
//            userNumMap.keySet().cursor().forEachForward((userId)->userIdSet.add(userId));
//        }
//        return userIdSet;
//    }

    public static BitSet getUiqueUserBitSet(ActivityVo[] activityVos){
        BitSet userIdBitSet = new BitSet(Constants.USER_ID_BOUND);
        for(ActivityVo activityVo : activityVos){
            HashIntIntMap userNumMap = activityVo.getUserNumMap();
            userNumMap.keySet().cursor().forEachForward(userIdBitSet::set);
        }
        return userIdBitSet;
    }


    private  static void mergeToMap2(HashLongObjMap<ActivityVo> activityMap, ActivityVo[] list){
        long t1 = System.currentTimeMillis();
        int delete = 0;
        for(ActivityVo vo : list){
            ActivityVo old = activityMap.get(vo.getActPlatfrom());
            if(old != null){
                old.merge(vo);
            }else{
                //舍弃
                delete ++;
            }
        }
        System.out.println("merge To map1个完成。舍弃远程结果数目：" + delete + ",耗时："+ (System.currentTimeMillis() - t1));
    }

    private  static void mergeToMap(HashLongObjMap<ActivityVo> activityMap, ActivityTransferVo[] list){
        long t1 = System.currentTimeMillis();
        int delete = 0;
        for(ActivityTransferVo vo : list){
            ActivityVo old = activityMap.get(vo.getActPlatfrom());
            if(old != null){
                old.merge(vo);
            }else{
                //舍弃
                delete ++;
            }
        }
        System.out.println("merge To map1个完成。舍弃远程结果数目：" + delete + ",耗时："+ (System.currentTimeMillis() - t1));
    }



    private static void sendStartToSlave(){
        //send 'start' to slave
        boolean msgSendSuccessd = false;
        do{
            try {
                new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.START, null);
                msgSendSuccessd = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (!msgSendSuccessd);

        msgSendSuccessd = false;
        do{
            try {
                new Client(Constants.SLAVE_IP_2, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.START, null);
                msgSendSuccessd = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (!msgSendSuccessd);
    }

//    private static void sendQueryFirstOrderToSlave(HashIntSet userIdSet){
//        //send 'start' to slave
//        boolean msgSendSuccessd = false;
//        do{
//            try {
//                new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdSet);
//                msgSendSuccessd = true;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } while (!msgSendSuccessd);
//
//        msgSendSuccessd = false;
//        do{
//            try {
//                new Client(Constants.SLAVE_IP_2, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdSet);
//                msgSendSuccessd = true;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        } while (!msgSendSuccessd);
//    }

    private static void sendUserOrderTimeQuery(BitSet userIdBitSet){
        new Thread(()->{
            boolean msgSendSuccessd = false;
            do{
                try {
                    new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdBitSet);
                    msgSendSuccessd = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } while (!msgSendSuccessd);
        }).start();

        new Thread(()->{
            boolean msgSendSuccessd = false;
            do{
                try {
                    new Client(Constants.SLAVE_IP_2, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.QUERY_FIRST_ORDER, userIdBitSet);
                    msgSendSuccessd = true;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            } while (!msgSendSuccessd);
        }).start();
    }
}
