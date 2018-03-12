package com.windlike.io;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import com.windlike.io.bio.Server;
import com.windlike.io.counter.NativeCounter;
import com.windlike.io.reader.*;
import com.windlike.io.util.MasterFirstOrderHandler;
import com.windlike.io.util.TopKComputer;
import com.windlike.io.vo.ActivityTransferVo;
import com.windlike.io.vo.ActivityVo;
import com.windlike.io.vo.Platforms;

import java.io.File;
import java.io.IOException;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.*;

/**
 * Created by windlike.xu on 2018/3/3.
 */
public class MasterApp {

    public static LinkedBlockingQueue<ActivityTransferVo[]> remoteTopN = new LinkedBlockingQueue<>(2<<1);

    public static void main(String[] args) throws InterruptedException {
//        if(args.length > 3){
//            Constants.DEFAULT_LIKE_GOOD_THREAD_NUM = Integer.parseInt(args[0]);//收藏数据
//            Constants.DEFAULT_FIRST_ORDER_THREAED_NUM = Integer.parseInt(args[1]);//首单数据
//            Constants.FILE_BUFFER_SIZE = Integer.parseInt(args[2]);//io buffer size
//            Constants.VALID_CANDIDATE_NUM = Integer.parseInt(args[3]);//topN
//            System.out.println("change params.");
//        }
        String result = fire();
        System.out.println(result);
        checkResult(result);
    }

    static Server server = new Server(Constants.SERVER_LISTENING_PORT);

    public static String fire() throws InterruptedException {
        long t1 = System.currentTimeMillis();


        System.out.println("App run in master mode");

        sendStartToSlave();

        //native counting start and get native topN result
        NativeCounter nativeCounter = new NativeCounter();
        nativeCounter.count();
        List<ActivityVo> activityList = nativeCounter.getActivityVos();
        nativeCounter = null;

        //get remote topN result
        long t11 = System.currentTimeMillis();
        HashLongObjMap activityMap = listToMap(activityList);

        mergeToMap(activityMap, remoteTopN.take());
        mergeToMap(activityMap, remoteTopN.take());

        long t2 = System.currentTimeMillis();

        System.out.println("mergeToMap两个完成，time:" + (t2 - t11));

        //cal top k
        ActivityVo[] topK = TopKComputer.computeTopKAfterMerge(activityMap, Constants.K);
        activityList = null;
        activityMap = null;

        //****首单计算*****/
        ExecutorService pool = Executors.newFixedThreadPool(Constants.DEFAULT_FIRST_ORDER_THREAED_NUM);

        //get user first time
//        HashIntSet userIdSet = getUiqueUserSet(topK);
        //初始化userIdBitSet
        BitSet userIdBitSet = getUiqueUserBitSet(topK);//// TODO: 2018/3/7

        //generate actAbstracts
        int[] actAbstracts = generateActAbstract(topK);
        MasterFirstOrderHandler.initSingleton(topK, actAbstracts);//初始化首单处理器

        File firstOrderPath = new File(Constants.FIRST_ORDER_FILE_PATH);
        String[] firstOrderFileNames = firstOrderPath.list();
        for (int i = 0; i < firstOrderFileNames.length; i++){
            pool.submit(new FirstOrderFileReaderByBuffer(Constants.FIRST_ORDER_FILE_PATH + firstOrderFileNames[i],
                    userIdBitSet, topK));
        }

        //发起首单查询到slave
        sendUserOrderTimeQuery(userIdBitSet);

        //等待所有首单文件处理结束
        MasterFirstOrderHandler.getInstance().waitAllComplete();

        long t7 = System.currentTimeMillis();

        System.out.println("总时间time:" + (t7-t1) + ",收藏数据耗时：" + (t2-t1) + ",首单耗时:"
                + (t7-t2));

        return getTopKResult(topK);
    }

    public static String getTopKResult(ActivityVo[] activityVos){
//        long userIdNotUnique = 0;
        String result = "";
        for (ActivityVo activityVo : activityVos){
//            System.out.println(activityVo);
            long actPlatform = activityVo.getActPlatfrom();
            HashIntIntMap userNumMap = activityVo.getUserNumMap();
            long actName = actPlatform >> 2;
            String platform = Platforms.indexToFullName((byte) (actPlatform & 3));
            result += actName+","+platform+","+activityVo.getAllNum()+","
                    +userNumMap.size()+","+(activityVo.getAllNum() - activityVo.getMinusNewUserAllTimes().get()) +
                    "," + (userNumMap.size() - activityVo.getMinusNewUserNum().get()) + "\n";
//            userIdNotUnique += activityVo.getUserNumMap().size();
        }

        return result;
//        System.out.println("\n unique userId size:" + userIdSet.size());
//        System.out.println("\n not unique userId size:" + userIdNotUnique);

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

    private static HashLongObjMap listToMap(List<ActivityVo> list){
        HashLongObjMap map = HashLongObjMaps.newUpdatableMap(list.size());
        for(ActivityVo vo : list){
            map.put(vo.getActPlatfrom(), vo);
        }
        return map;
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



    public static void checkResult(String result){
        String str = "20171129005051848,app,47557567,46628040,35798233,35098503\n" +
                "20171111235011797,app,2753726,2750513,2085102,2082657\n" +
                "20171130231520505,app,2366836,2364526,1781004,1779284\n" +
                "20171210014335918,pc,2309213,2307023,1735943,1734296\n" +
                "20170727193540953,app,2080794,2078974,1623146,1621760\n" +
                "20170331182008610,app,1999107,1997488,1606003,1604700\n" +
                "20171029042203127,weixin,1904393,1902842,1444237,1443043\n" +
                "20171126135002327,app,1818569,1817240,1370780,1369769\n" +
                "20171029042203127,wap,1725910,1724690,1309657,1308717\n" +
                "20170202194354578,wap,1499252,1498330,1222135,1221420\n" +
                "20171130192311717,app,1231841,1231248,926748,926308\n" +
                "20170202194354578,pc,1134668,1134085,925320,924845\n" +
                "20170331182008610,wap,1109182,1108674,891368,890961\n" +
                "20171201220520971,app,1092684,1092252,823033,822693\n" +
                "20171202024926830,app,1052208,1051751,791500,791162\n" +
                "20171204032900656,app,1017987,1017510,767656,767304\n" +
                "20171204072103261,app,955297,954922,719338,719054\n" +
                "20171204001230008,app,954775,954412,718413,718139\n" +
                "20171122001308924,app,926790,926415,698581,698294\n" +
                "20171124173159701,app,878039,877752,662239,662019\n" +
                "20171127010923441,app,844325,844035,636229,636017\n" +
                "20171204152445252,app,843544,843243,635183,634962\n" +
                "20171206192438789,app,807519,807239,606934,606725\n" +
                "20170202194354578,app,788013,787765,642433,642219\n" +
                "20171203103546885,app,766018,765748,577575,577365\n" +
                "20171123171557217,app,735849,735643,554995,554847\n" +
                "20171029042203127,pc,718533,718318,544719,544553\n" +
                "20171210014335918,wap,713138,712921,535966,535802\n" +
                "20171208092418382,app,693115,692906,519669,519517\n" +
                "20171112173538096,app,688431,688222,519959,519809\n" +
                "20171208232417917,app,668843,668656,503234,503097\n" +
                "20171130220351273,app,646825,646641,487676,487538\n" +
                "20171204082913725,app,642880,642711,484500,484379\n" +
                "20171209123947715,app,626904,626743,470495,470379\n" +
                "20171029042203127,app,623082,622928,472357,472233\n" +
                "20171111235011797,wap,622412,622241,471465,471334\n" +
                "20170727193540953,wap,590718,590597,460409,460320\n" +
                "20171118123940092,app,587499,587359,443900,443792\n" +
                "20171210201120994,app,587114,586973,440366,440256\n" +
                "20171207191534580,app,584267,584120,439916,439813\n";

        if(result.equals(str)){
            System.out.println("答案全部正确！！！！！！！！！！！！！！！！");
        }else{
            System.out.println("答案错误！！！！！！！！！！！！！！！！");
        }
    }
}
