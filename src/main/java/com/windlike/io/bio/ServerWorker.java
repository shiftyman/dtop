package com.windlike.io.bio;

import com.windlike.io.Constants;
import com.windlike.io.MasterApp;
import com.windlike.io.counter.NativeCounter;
import com.windlike.io.reader.FirstOrderFileReaderByBuffer;
import com.windlike.io.util.MasterFirstOrderHandler;
import com.windlike.io.vo.ActivityTransferVo;
import com.windlike.io.vo.ActivityVo;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class ServerWorker implements Runnable{

    private Socket socket;

    public ServerWorker(Socket socket){
        this.socket = socket;
    }

    @Override
    public void run() {
        try (DataInputStream input = new DataInputStream(
                new BufferedInputStream(socket.getInputStream()))) {

            long t1 = System.currentTimeMillis();

            byte msgType = input.readByte();
            if(msgType == Protocol.MsgType.START.ordinal()){//slave
                NativeCounter nativeCounter = new NativeCounter();
                nativeCounter.count();

                //send to master
                sendTopNToMaster(nativeCounter.getActivityVos());
            }else if(msgType == Protocol.MsgType.QUERY_FIRST_ORDER.ordinal()){//slave
                //获取消息内容
                int size = input.readInt();
//                HashIntSet userIdSet = HashIntSets.newUpdatableSet(size);
//                IoUtil.readQueryFirstOrder(input, userIdSet, size);

                //！！！！！改为bitset
                BitSet userIdBitSet = IoUtil.readQueryFirstOrder(input, size);

                System.out.println("首单查询接收time:" + (System.currentTimeMillis() - t1) + "," +
                        "userIdsize:" + size);

                //首单处理 slave
                ExecutorService pool = Executors.newFixedThreadPool(Constants.DEFAULT_FIRST_ORDER_THREAED_NUM);
                File firstOrderPath = new File(Constants.FIRST_ORDER_FILE_PATH);
                String[] firstOrderFileNames = firstOrderPath.list();
//                List<Future> futures = new ArrayList<>(firstOrderFileNames.length);
                for (int i = 0; i < firstOrderFileNames.length; i++){
                    pool.submit(new FirstOrderFileReaderByBuffer(Constants.FIRST_ORDER_FILE_PATH + firstOrderFileNames[i],
                            userIdBitSet, Constants.DEFAULT_FIRST_ORDER_THREAED_NUM));
                }
            }else if(msgType == Protocol.MsgType.RESPONSE_FIRST_ORDER.ordinal()){//master
                int size = input.readInt();
                int actualSize  = size * 2;
                int[] data = new int[actualSize];
                for(int i = 0; i < actualSize; i++){
                    data[i] = input.readInt();
                }

                long tt1 = System.currentTimeMillis();
                System.out.println("首单响应接收time:" + (tt1 - t1) + "," +
                        "userSize:" + size);

                //读取完毕，处理
                for(int j = 0; j < actualSize; j=j+2){
                    MasterFirstOrderHandler.getInstance().processFirstOrder(data[j], data[j+1]);
                }

                long tt2 = System.currentTimeMillis();
                System.out.println("首单响应处理time:" + (tt2 - tt1));

                MasterFirstOrderHandler.getInstance().finishOne();
            }else if(msgType == Protocol.MsgType.REPORT_TOPK.ordinal()){//master
                //活动数量short、活动名平台long、收藏总数int、
                // 开始时间int、结束时间int、 独立用户数量int、userintems（userid-int、此user个数int）
                short actSize = input.readShort();
//                ActivityVo[] activityVos = new ActivityVo[actSize];
                ActivityTransferVo[] transferVos = new ActivityTransferVo[actSize];
                for(int i = 0; i < actSize;i ++){
                    long actPlatform = input.readLong();
                    int allNum = input.readInt();
//                    int startTime = input.readInt();
//                    int endTime = input.readInt();
                    int userSize = input.readInt();
//                    ActivityVo activityVo = new ActivityVo(actPlatform, 0, 0,
//                            allNum, userSize);
//                    HashIntIntMap userNumMap = activityVo.getUserNumMap();


//                    IoUtil.readTopNUser(input, userNumMap, userSize);
//                    activityVos[i] = activityVo;


                    ActivityTransferVo activityTransferVo = new ActivityTransferVo(actPlatform, allNum, userSize);
                    IoUtil.readTopNUser(input, activityTransferVo.getUserNumList(), userSize);
                    transferVos[i] = activityTransferVo;
                }
                MasterApp.remoteTopN.add(transferVos);
            }else if(msgType == Protocol.MsgType.TEST.ordinal()){
//                int size = input.readInt();
//                for(int i =0 ; i<size * 2048L;i++){
//                    input.read();
//                }
//                for(int i =0 ; i<size;i++){
////                    input.readInt();
//                    input.readInt();
//                }
                int bufsize = 1024;
                byte[] bytes = new byte[bufsize];
                int length;
//                int lineLength = 0;
                int off = 0;
                long readTime = 0;
                do{
                    length = input.read(bytes, off, bufsize - off);
                    off += length;
                    if(off == bufsize){
//                        off = lineLength;
//                    }else{
                        //读取
                        for(int i = 0; i < bufsize; i+=4){
                            int data = ((bytes[i] << 24) + (bytes[i+1] << 16) +
                                    (bytes[i+2] << 8) + (bytes[i+3] << 0));
                            readTime ++;
                        }
                        off = 0;
//                        lineLength = 0;
                    }
                }while (length > 0);

                if(off > 0){
                    for(int i = 0; i < off; i+=4){
                        int data = ((bytes[i] << 24) + (bytes[i+1] << 16) +
                                (bytes[i+2] << 8) + (bytes[i+3] << 0));
                        readTime ++;
                    }
                }



                System.out.println(readTime + ",个读取");

            }

            long t2 = System.currentTimeMillis();
            System.out.println("消息处理完毕。msgType：" + msgType + ",time:" + (t2-t1));
        } catch (Exception e) {
            e.printStackTrace();
            //意外，需要抛弃此次处理的数据，等待客户端再次连接并发送
        }finally {
            try {
                if(socket != null) socket.close();
                System.out.println("server socket closed.");
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        }
    }

    private void sendTopNToMaster(List<ActivityVo> activityList){
        boolean msgSendSuccessd = false;
        do{
            try {
                new Client(Constants.MASTER_IP, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.REPORT_TOPK, activityList);
                msgSendSuccessd = true;
            } catch (IOException e) {
                e.printStackTrace();
            }
        } while (!msgSendSuccessd);
    }

    public static void main(String[] args) {
        int start = 131;
        int end = 128;
        int index = 40000;
        long x = ((long)start << 32) + ((long)end << 16) + (long)index;
        int t1 = (int) (x & 0xffff);
        int t2 = (int) ((x-index) >>> 16) & 0xffff;
        int t3 = (int) (x >>> 32);
        System.out.println(x);
        System.out.println(t1);//index
        System.out.println(t2);//end
        System.out.println(t3);//start
    }

}
