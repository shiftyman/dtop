package com.windlike.io.reader;

import com.windlike.io.Constants;
import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.util.MasterFirstOrderHandler;
import com.windlike.io.vo.ActivityVo;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongList;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.util.*;

/**
 * Created by windlike.xu on 2018/3/4.
 */
public class FirstOrderFileReaderByBuffer implements Runnable{

    private String orignFileFullName;

    private BitSet bitSet;

    private int row = 0;//行数

    private int mode = 0; // 0 - master, 1 -slave

    //MASTER
    private ActivityVo[] topK = null;//仅在master模式下使用

    //SALVE
    private LongList dataList;//salve使用，每个reader先读取并保存数据，最后一次过发送

    /**
     * slave模式
     * @param orignFileFullName
     * @param userIdSet
     */
    public FirstOrderFileReaderByBuffer(String orignFileFullName, BitSet userIdSet, int threadNum) {
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
    public FirstOrderFileReaderByBuffer(String orignFileFullName, BitSet userIdSet, ActivityVo[] topk) {
        this.orignFileFullName = orignFileFullName;
        this.bitSet = userIdSet;
        this.topK = topk;
//        this.counter = counter;//当counter==0时，程序输出结果
//        this.actAbstracts = actAbstracts;
//        this.mutex = mutex;
    }

    public void run() {
        long t1 = System.currentTimeMillis();

        try (BufferedReader br = new BufferedReader(new FileReader(new File(orignFileFullName)), Constants.FILE_BUFFER_SIZE)){
            String str = null;
            while((str = br.readLine()) != null) {
                row++;

//                List<String> pieces = InvincibleConvertUtil.split(str, ',', 2);
                String[] pieces = InvincibleConvertUtil.split(str, ',', 2);
                int userId = InvincibleConvertUtil.stringToInt(pieces[0]);
                if(!bitSet.get(userId)){
                    continue;
                }
                int firstOrderTime;
                String date = pieces[1];
                if(date.charAt(2) != Constants.ONE_ASCII || date.charAt(3) != Constants.SEVEN_ASCII){
                    firstOrderTime = 0;
                }else{
                    firstOrderTime = InvincibleConvertUtil.dateToMMddhhmmss(date);
                }

                //后续处理
                doProcess(userId, firstOrderTime);
            }

            //处理完毕后处理
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

        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.out.println("此first order任务完成，解释行数：" + row + ",time:" + (System.currentTimeMillis() - t1));
        }
    }


    private void doProcess(int userId, int firstOrderTime){
        if(mode == 0){//master模式直接计算
            MasterFirstOrderHandler.getInstance().processFirstOrder(userId, firstOrderTime);
//            if(counter.decrementAndGet() <= 0){
//                System.out.println("提前结束@@@@");
////                mutex.notify();//提前结束?真的有用么亲?~
//            }
        }else{//slave模式，发送给master
            long data = ((long)userId << 32) + firstOrderTime;
            dataList.add(data);
        }
    }


}
