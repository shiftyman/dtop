//package com.windlike.io.util;
//
//import com.windlike.io.Constants;
//
///**
// * Created by windlike.xu on 2018/3/1.
// */
//public class TestFileRead {
//
//    public static void main(String[] args) throws InterruptedException {
//
////        int i = 1;
////        while (true){
////            Thread.sleep(5000L);
////            i++;
////            if(i > 2){
////                break;
////            }
////        }
////
////        long t1 = System.currentTimeMillis();
////        ActivityFileReader activityFileReader = new ActivityFileReader(Constants.ACT_DATA_FILE);
////        activityFileReader.call();
////
////        long t2 = System.currentTimeMillis();
////
////        ActivityFileReader2 activityFileReader2 = new ActivityFileReader2(Constants.ACT_DATA_FILE);
////        activityFileReader2.call();
////
////        long t3 = System.currentTimeMillis();
////
////        System.out.println("1:" + (t2-t1) + ",2:" + (t3-t2));
//
//        long t1 = System.currentTimeMillis();
//        int v = 10;
//        for (int i = 0; i < 100000000000L;i++){
//            byte b1 = (byte) ((v >>> 24) & 0xFF);
//            byte b2 = (byte)((v >>> 16) & 0xFF);
//            byte b3 = (byte)((v >>>  8) & 0xFF);
//            byte b4 = (byte)((v >>>  0) & 0xFF);
//        }
//        long t2 = System.currentTimeMillis();
//        System.out.println(t2-t1);
//
//
//    }
//}
