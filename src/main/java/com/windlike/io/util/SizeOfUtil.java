package com.windlike.io.util;

import com.carrotsearch.sizeof.RamUsageEstimator;

import static java.lang.System.out;

/**
 * Created by windlike.xu on 2018/1/12.
 */
public class SizeOfUtil {

    public static void printSize(Object o) {
        out.printf("类型：%s，占用内存：%.2f MB\n", o.getClass().getSimpleName(), RamUsageEstimator.sizeOf(o) / 1024D / 1024D);
    }

    public static void main(String[] args) {
//        char c = '2';
//        int i = Character.digit(c, 10);
//        System.out.println(i);
//        System.out.println(charToNum(c));

        String str = "1235627";
        System.out.println(stringToInt(str));

        long t1 = System.currentTimeMillis();
        for (int j = 0; j < 100000000; j++) {
//            int i = Character.digit(c, 10);
//           int i = stringToInt2(str);
//            int i = Integer.parseInt(str);
        }
        long t2 = System.currentTimeMillis();

        for (int j = 0; j < 100000000; j++) {
//            int i = charToNum(c);
            int i = stringToInt(str);
        }

        long t3 = System.currentTimeMillis();

        System.out.println((t2-t1)+"," + (t3-t2));


    }

    private static int stringToInt(String str){
        int sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = sum * 10 + ((int) str.charAt(i) - 48);
        }
        return sum;
    }


}
