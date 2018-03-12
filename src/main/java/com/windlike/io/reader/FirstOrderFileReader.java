package com.windlike.io.reader;

import com.koloboke.collect.map.ObjByteCursor;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.MasterApp;
import com.windlike.io.Platforms;
import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.util.MasterFirstOrderHandler;
import com.windlike.io.vo.ActivityVo;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by windlike.xu on 2018/3/4.
 */
public class FirstOrderFileReader implements Runnable{

    private String orignFileFullName;

//    private HashIntSet userIdSet;

    private BitSet bitSet;

    private int row = 0;//行数

    private int mode = 0; // 0 - master, 1 -slave


    //MASTER
    private ActivityVo[] topK = null;//仅在master模式下使用
//    private AtomicInteger counter;//counter 仅在master模式下使用
    private int[] actAbstracts;//活动信息摘要[MMddMMdd] 仅在master模式下使用
//    private Object mutex;//仅在master模式下使用

    //SALVE
    private LongList dataList;//salve使用，每个reader先读取并保存数据，最后一次过发送

    /**
     * slave模式
     * @param orignFileFullName
     * @param userIdSet
     */
    public FirstOrderFileReader(String orignFileFullName, BitSet userIdSet, int threadNum) {
        this.orignFileFullName = orignFileFullName;
        this.bitSet = userIdSet;
        this.mode = 1;
        this.dataList = new LongArrayList(Constants.SUPPOSE_TOPK_USERIDS / 3 / threadNum);//预估单节点用户4000w // TODO: 2018/3/5
    }

    /**
     * master模式
     * @param orignFileFullName
     * @param userIdSet
     * @param topk
     */
    public FirstOrderFileReader(String orignFileFullName, BitSet userIdSet, ActivityVo[] topk,
                                int[] actAbstracts) {
        this.orignFileFullName = orignFileFullName;
        this.bitSet = userIdSet;
        this.topK = topk;
//        this.counter = counter;//当counter==0时，程序输出结果
        this.actAbstracts = actAbstracts;
//        this.mutex = mutex;
    }

    public void run() {
        long t1 = System.currentTimeMillis();

        //启动读写任务
        File file  = new File(orignFileFullName);
        try (FileInputStream orginFile = new FileInputStream(file)){
            MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());
            readAndProcess(inputBuffer);

            if(mode == 1){//slave模式
                //发送数据
                boolean msgSendSuccessd = false;
                do{
                    try {
                        new Client(Constants.MASTER_IP, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.RESPONSE_FIRST_ORDER, dataList);
                        msgSendSuccessd = true;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } while (!msgSendSuccessd);
            }else{//master模式
                MasterFirstOrderHandler.getInstance().finishOne();
            }


            //清理
            bitSet = null;
            actAbstracts = null;
            topK = null;

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("此first order任务完成，解释行数：" + row + ",time:" + (System.currentTimeMillis() - t1));
        }


    }

    public void readAndProcess(MappedByteBuffer byteBuffer){
        byte[] fileLineCache = new byte[50];
        int length;

        int userId;
        int firstOrderTime;
        while (byteBuffer.hasRemaining()){
            row++;

            length = getSegment(byteBuffer, fileLineCache);
            userId = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, length);
//            if(!userIdSet.contains(userId)){
            if(!bitSet.get(userId)){
                //skip
                byteBuffer.position(byteBuffer.position() + 20);
                continue;
            }

            byteBuffer.position(byteBuffer.position() + 2);
            byte y1 = byteBuffer.get();
            byte y2 = byteBuffer.get();
            if(y1 != Constants.ONE_ASCII || y2 != Constants.SEVEN_ASCII){
                byteBuffer.position(byteBuffer.position() + 16);
                firstOrderTime = 0;//17年前,无效
            }else{
                //parse date
                byteBuffer.position(byteBuffer.position() + 1);
                byteBuffer.get(fileLineCache, 0, 2);
                byteBuffer.position(byteBuffer.position() + 1);
                byteBuffer.get(fileLineCache, 2, 2);
                byteBuffer.position(byteBuffer.position() + 1);
                byteBuffer.get(fileLineCache, 4, 2);
                byteBuffer.position(byteBuffer.position() + 1);
                byteBuffer.get(fileLineCache, 6, 2);
                byteBuffer.position(byteBuffer.position() + 1);
                byteBuffer.get(fileLineCache, 8, 2);
                byteBuffer.position(byteBuffer.position() + 1);

                firstOrderTime = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, 10);
            }

            //后续处理
            doProcess(userId, firstOrderTime);
        }

    }

    private void doProcess(int userId, int firstOrderTime){
        if(mode == 0){//master模式直接计算
//            for (int i = 0; i < actAbstracts.length; i++){
//                int endTime = actAbstracts[i];
////                int tx  = abs / 1000000 * 1000000;
////                int startTime = tx + 100000;
////                int endTime = (abs - tx) / 100 * 1000000 + 95959;
//                if(firstOrderTime <= endTime){//老客，削减
//                    if(topK[i].containsUser(userId)){
//                        topK[i].findAnOldUser(userId);
//                    }
//                }
//            }
            MasterFirstOrderHandler.getInstance().processFirstOrder(userId, firstOrderTime);
//            if(counter.decrementAndGet() <= 0){
//                System.out.println("提前结束@@@@");
////                mutex.notify();//提前结束?真的有用么亲?~
//            }
        }else{//slave模式，发送给master
            long data = ((long)userId << 32) + firstOrderTime;

//            boolean msgSendSuccessd = false;
//            do{
//                try {
//                    new Client(Constants.MASTER_IP, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.RESPONSE_FIRST_ORDER, data);
//                    msgSendSuccessd = true;
//                } catch (IOException e) {
//                    e.printStackTrace();
//                }
//            } while (!msgSendSuccessd);

            dataList.add(data);
        }
    }

    private int getSegment(MappedByteBuffer inputBuffer, byte[] fileLineCache){
        int i = 0;
        try{
            for(; i < fileLineCache.length; i++){
                byte b = inputBuffer.get();
                if(b == Constants.SEGMENT_SEPARATE_SYMBOL || b == Constants.NEW_LINE_CHAR_ASCII){
                    break;
                }
                fileLineCache[i] = b;
            }
        }catch (BufferUnderflowException e){
            //已经是结尾
        }
        return i;
    }


    public static void main(String[] args) {
        int userid =Integer.MAX_VALUE;
        int firstO = Integer.MAX_VALUE;
        long data = ((long)userid << 32) + firstO;


        int userId = (int) (data >> 32);
        int firstOrderTime = (int) (data & Integer.MAX_VALUE);
        System.out.println(userId + "," + firstOrderTime);
    }


    public static void main2(String[] args) {
        int startTime = 530100000;
        int endTime = 1130100000;
        ArrayList<ActivityVo> activityVos = new ArrayList<>(40);
        for(int i = 0; i < 40; i ++){
            activityVos.add(new ActivityVo(i, startTime + i *100000000, endTime + i * 100000000, 0, 0));
        }
        int tmp = 630878970;
        long t1 = System.currentTimeMillis();
//        for (int i = 0; i < 80000000; i++){
//            for (ActivityVo vo : activityVos){
//                if(vo.getStartTime() <= tmp && vo.getEndTime() >=tmp){
//
//                }
//            }
//        }

        long t2 = System.currentTimeMillis();

        System.out.println("time:" + (t2-t1));

        Random random = new Random();

        int[] array = new int[40];
        ArrayList<Integer> list = new ArrayList<>(40);
        for(int i = 0; i < 40; i ++){
//            array[i] = (530 + i * 100) * 1000000 + (1130 + i * 100) * 100 + 1;
//            array[i] =
            list.add(random.nextInt(1000000000) + 1000000000);
        }

        long t3 = System.currentTimeMillis();

        Collections.sort(list);
        for (int i = 0; i < 40; i++) {
            array[i] = list.get(i);
        }

        for (int i = 0; i < 80000000; i++){
            for(int k = 0; k < 40; k ++){
                int tx  = array[k] / 1000000 * 1000000;
                int st = tx + 100000;
                int et = (array[k] - tx) / 100 * 1000000 + 95959;
                if(tmp >= startTime && tmp <= endTime){

                }
            }
        }

        long t4 = System.currentTimeMillis();

        System.out.println(t4-t3);
    }
}
