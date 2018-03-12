package com.windlike.io.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by windlike.xu on 2018/2/14.
 */
public class InvincibleConvertUtil {

    public static long stringToLong(String str){
        long sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = sum * 10 + ((byte) str.charAt(i) - 48);
        }
        return sum;
    }

    public static int stringToInt(String str){
        int sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = sum * 10 + ((byte) str.charAt(i) - 48);
        }
        return sum;
    }

    public static long byteArrayToLong(byte[] bytes, int start, int length){
        long sum = 0;
        for(int i = start; i < length; i ++){
            sum = sum * 10 + (bytes[i] - 48);
        }
        return sum;
    }

    public static int byteArrayToInt(byte[] bytes, int start, int length){
        int sum = 0;
        for(int i = start; i < length; i ++){
            sum = sum * 10 + (bytes[i] - 48);
        }
        return sum;
    }

    public static byte stringToByte(String str){
        byte sum = 0;
        for(int i = 0; i < str.length(); i ++){
            sum = (byte) (sum * 10 + ((byte) str.charAt(i) - 48));
        }
        return sum;
    }

    public static List<String> split2(String str, char separatorChar, int expectParts) {
        if(str == null) {
            return null;
        } else {
            int len = str.length();
            if(len == 0) {
                return null;//new String[0];
            } else {
                ArrayList<String> list = new ArrayList(expectParts);
                int i = 0;
                int start = 0;
                boolean match = false;

                while(i < len) {
                    if(str.charAt(i) == separatorChar) {
                        if(match) {
                            list.add(str.substring(start, i));
                            match = false;
                        }

                        ++i;
                        start = i;
                    } else {
                        match = true;
                        ++i;
                    }
                }

                if(match) {
                    list.add(str.substring(start, i));
                }

//                return list.toArray(new String[list.size()]);
                return list;
            }
        }
    }


    public static String[] split(String str, char separatorChar, int expectParts) {
        if(str == null) {
            return null;
        } else {
            int len = str.length();
            if(len == 0) {
                return null;//new String[0];
            } else {
//                ArrayList<String> list = new ArrayList(expectParts);
                String[] list = new String[expectParts];
                int off = 0;
                int i = 0;
                int start = 0;
                boolean match = false;

                while(i < len) {
                    if(str.charAt(i) == separatorChar) {
                        if(match) {
//                            list.add(str.substring(start, i));
                            list[off] = str.substring(start, i);
                            off++;
                            match = false;
                        }

                        ++i;
                        start = i;
                    } else {
                        match = true;
                        ++i;
                    }
                }

                if(match) {
//                    list.add(str.substring(start, i));
                    list[off] = str.substring(start, i);
                    off++;
                }

//                return list.toArray(new String[list.size()]);
                return list;
            }
        }
    }



    public static void main(String[] args) {
        String str = "sdfdsf sdfds sdf xvcc sdfsd werew";

        long t2 = System.currentTimeMillis();

        for (int j = 0; j < 100000000; j++) {
//            int i = charToNum(c);
            split(str, ' ', 10);
        }

        long t3 = System.currentTimeMillis();

        System.out.println((t3-t2));

    }

    public static int dateToMMddhhmmss(String date){
        return ((byte)date.charAt(5) - 48) * 1000000000 + ((byte)date.charAt(6) - 48) * 100000000 + ((byte)date.charAt(8) - 48) * 10000000 + ((byte)date.charAt(9) - 48) * 1000000
                + ((byte)date.charAt(11) - 48) * 100000 + ((byte)date.charAt(12) - 48) * 10000 + ((byte)date.charAt(14) - 48) * 1000 + ((byte)date.charAt(15) - 48) * 100 +
                ((byte)date.charAt(17) - 48) * 10 + ((byte)date.charAt(18) - 48);

//        StringBuilder str = new StringBuilder();
//        str.append(date.charAt(5)).append(date.charAt(6)).append(date.charAt(8)).append(date.charAt(9))
//                .append(date.charAt(11)).append(date.charAt(12)).append(date.charAt(14)).append(date.charAt(15)).append(date.charAt(17)).append(date.charAt(18));
//        return Integer.parseInt(str.toString());
    }
}
