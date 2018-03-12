package com.windlike.io.nio;

import com.windlike.io.util.InvincibleConvertUtil;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.BitSet;
import java.util.Date;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class TestApp {

    public static void main(String[] args) throws ParseException {
//        new NioServer(8080).start();
//
//        while (true){
//            try {
//                Thread.currentThread().sleep(10000L);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//
//        long t1 = System.currentTimeMillis();
////        for (int i = 0 ; i < 300000; i ++){
////            Date date = simpleDateFormat.parse("2017-03-02 10:00:00");
////            long ts = date.getTime();
////        }
//
//        long t2 = System.currentTimeMillis();
////        System.out.println(t2-t1);
//
////
//        long l = Long.parseLong(args[0]);
//        for (int i = 0 ; i < 300000; i ++){
//            long t = InvincibleConvertUtil.stringToLong(args[0].substring(4, 8) + "100000");
////            long t = (l / 1000000000 - 20170000) * 1000000 + 100000;
////            System.out.println(t);
//        }
//        long t3 = System.currentTimeMillis();
//
//
//        System.out.println(t3-t2);

        BitSet bitSet = new BitSet(64);
        bitSet.set(2);
        bitSet.set(8);
        byte[] bytes = bitSet.toByteArray();
        long[] long1 = bitSet.toLongArray();

        int off = 0;

//        long[] longArray = new long[1];
//        for (int i = 0 ; i < bytes.length; i += 8){
//            longArray[off] = (((long)bytes[i] << 56) +
//                    ((long)(bytes[i+1] & 255) << 48) +
//                    ((long)(bytes[i+2] & 255) << 40) +
//                    ((long)(bytes[i+3] & 255) << 32) +
//                    ((long)(bytes[i+4] & 255) << 24) +
//                    ((bytes[i+5] & 255) << 16) +
//                    ((bytes[i+6] & 255) <<  8) +
//                    ((bytes[i+7] & 255) <<  0));
//            off++;
//        }

        BitSet bitSet1 = BitSet.valueOf(long1);
        System.out.println(bitSet1 + "," + bitSet);
    }
}
