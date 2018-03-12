package com.windlike.io.bio;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.windlike.io.vo.ActivityVo;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongListIterator;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.BitSet;
import java.util.List;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class Client {

    private Socket socket;

    public Client(String host, int port){
        try {
            socket = new Socket(host, port);
//            socket.setReceiveBufferSize(Constants.NETWORK_BUFFER_SIZE);
//            socket.setSendBufferSize(Constants.NETWORK_BUFFER_SIZE);
            socket.setTrafficClass(0x08);
            socket.setTcpNoDelay(true);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public void send(Protocol.MsgType msgType, Object payload) throws IOException {
        long t1 = System.currentTimeMillis();
        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
//        try (DataOutputStream output = new DataOutputStream((socket.getOutputStream()
//            ))) {
            output.writeByte(msgType.ordinal());//header
            if(msgType == Protocol.MsgType.START){
                //empty
            }else if(msgType == Protocol.MsgType.QUERY_FIRST_ORDER){
                BitSet userIdBitSet = (BitSet) payload;
                IoUtil.writeQueryFirstOrder(output, userIdBitSet);
            }else if(msgType == Protocol.MsgType.RESPONSE_FIRST_ORDER){
                LongArrayList datas = (LongArrayList)payload;
                output.writeInt(datas.size());
//                long data = (long)payload;
                LongListIterator iterator = datas.iterator();
                while (iterator.hasNext()){
                    long data = iterator.nextLong();
                    int userId = (int) (data >> 32);
                    int firstOrderTime = (int) (data & Integer.MAX_VALUE);
                    output.writeInt(userId);
                    output.writeInt(firstOrderTime);
                }
            }else if(msgType == Protocol.MsgType.REPORT_TOPK){
                //协议：活动数量short、活动名平台long、收藏总数int、//开始时间int、结束时间int去除!!!//、
                //独立用户数量int、userintems（userid-int、此user个数int）
                List<ActivityVo> activityList = (List<ActivityVo>) payload;
                output.writeShort(activityList.size());
//                Iterator<ActivityVo> iterator = activityList.values().iterator();
//                while (iterator.hasNext()){
                for (ActivityVo vo : activityList){
//                    ActivityVo vo = iterator.next();
                    output.writeLong(vo.getActPlatfrom());
                    output.writeInt(vo.getAllNum());
                    HashIntIntMap userMap = vo.getUserNumMap();
                    output.writeInt(userMap.size());
                    IoUtil.writeTopNUser(output, userMap);
                }
            }else if(msgType == Protocol.MsgType.TEST){
                int size = (int)payload;
//                output.writeInt(size);
//                int s=2048;
//                byte[] bytes = new byte[s];
//                for (int i = 0 ; i< s;i++){
//                    bytes[i] = 2;
//                }
                int bufsize = 1024;
                byte[] bytes = new byte[bufsize];
                int v = 1;
                int j = 0;
                long writeTime = 0;
                for (int i=0;i<size;i++) {
                    bytes[j] = (byte) ((v >>> 24) & 0xFF);
                    bytes[j + 1] = (byte) ((v >>> 16) & 0xFF);
                    bytes[j + 2] = (byte) ((v >>> 8) & 0xFF);
                    bytes[j + 3] = (byte) ((v >>> 0) & 0xFF);
                    j+=4;
                    if(j == bufsize){
                        output.write(bytes);
                        j=0;
                        writeTime++;
                    }
//                    output.writeInt(1);
                }
                if(writeTime < size){
                    output.write(bytes, 0, j);
                }

                System.out.println(writeTime + "个发送数据");
            }

            output.flush();
            System.out.println("发送完毕.msgType:" + msgType.name() + ",time:" + (System.currentTimeMillis() - t1));
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        } finally {
            try {
                if(socket != null) socket.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

//    public void start() throws IOException, InterruptedException {
//        try (DataOutputStream output = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()))) {
//            //协议：品牌数量short、品牌名size-short、品牌名string、总数int、开始时间long、结束时间long、品牌id-long、
//            // 平台byte、独立用户数量int、userintems（userid-long、此user个数short）
//            output.writeShort(brandList.size());
//            for (BrandTransferVo brand : brandList){
//                byte[] bytes = brand.getBrandName().getBytes();
//                output.writeShort(bytes.length);
//                output.write(bytes);
//                output.writeInt(brand.getAllNum());
//                output.writeLong(brand.getStartTime());
//                output.writeLong(brand.getEndTime());
//                output.writeLong(brand.getBrandId());
//                output.writeByte(brand.getPlatform());
//
////                long[] userInfo = brand.getUserIdAndNum();
////                output.writeInt(userInfo.length >> 1);
////                for (int i = 0 ; i < userInfo.length; i=i+2){
////                    output.writeLong(userInfo[i]);
////                    output.writeShort((int) userInfo[i+1]);
////                }
//
////                output.flush();
////                break;
////                Thread.sleep(5000L);
//            }
//            output.flush();
//
//            System.out.println("发送完毕");
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw e;
//        } finally {
//            try {
//                if(socket != null) socket.close();
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        }
//    }

//    public static void main(String[] args) {
//        try {
//            new Client("127.0.0.1", 8080).start();
//        } catch (Exception e) {
//            e.printStackTrace();
//
//            //重试1次
//            try {
//                new Client("127.0.0.1", 8080).start();
//            } catch (IOException e1) {
//                e1.printStackTrace();
//            } catch (InterruptedException e1) {
//                e1.printStackTrace();
//            }
//        }
//    }


}
