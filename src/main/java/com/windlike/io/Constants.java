package com.windlike.io;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.windlike.io.util.SizeOfUtil;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntListIterator;
import it.unimi.dsi.fastutil.ints.IntLists;
import it.unimi.dsi.fastutil.longs.LongListIterator;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Map;
import java.util.Random;

/**
 * Created by windlike.xu on 2018/2/28.
 */
public class Constants {

    public static final byte SENT_FLAG = (byte) 0xf0;

    public static final byte NOT_SENT_FLAG = (byte) 0x00;

    public static final int DEFAULT_ACT_SIZE = 20000;

    /**
     * 换行符ascii
     */
    public final static int NEW_LINE_CHAR_ASCII = 10;

    public final static int SEGMENT_SEPARATE_SYMBOL = 44;

    public final static int EOF_FILE = -1;

    public final static int SEVEN_ASCII = 55;//7

    public final static int ONE_ASCII = 49;

    //// TODO: 2018/3/6 生产时data 
    public final static String ACT_DATA_FILE = "/data/dty_act_warmup_brand_total_hm/0-0.txt";

    public final static String LIKE_GODDS_DATA_FILE_PATH = "/data/trd_brand_goods_like_hm/";

    public final static String FIRST_ORDER_FILE_PATH = "/data/dm_vip_user_first_ord_dt/";

    public final static int VALID_CANDIDATE_NUM = 200;

    public final static int K = 40;

    public final static int MACHINE = 3;

    public final static int SERVER_LISTENING_PORT = 8085;

    public final static String SLAVE_IP_1 = "10.191.24.12";

    public final static String SLAVE_IP_2 = "10.191.24.13";

    public final static String MASTER_IP = "10.191.24.11";

    public final static int SUPPOSE_TOPK_USERIDS = 40000000;//4000w * 2 * 4 = 320M存储

    public final static int SUPPOSE_ACTIVITY_UNIQUE_USER_NUM = 8000;//// TODO: 2018/3/6  待评估

    public final static int DEFAULT_LIKE_GOOD_THREAD_NUM = 8;

    public final static int DEFAULT_FIRST_ORDER_THREAED_NUM = 8;

    public final static int FILE_SPLIT_NUM = 100;//10;//100;// TODO: 2018/3/6 生产时100

    public final static int NETWORK_BUFFER_SIZE = 8192 * 5;

    public final static int USER_ID_BOUND = 300 * 10000 * 100 + 10;

    public static void main2(String[] args) {
//        long actPlatform = 80681688899415834L;
//        System.out.println(Platforms.indexToFullName((byte) (actPlatform & 3)));
//        System.out.println(actPlatform >> 2);

        long t1 = System.currentTimeMillis();

        Random random = new Random();
        HashIntIntMap map1 = HashIntIntMaps.newUpdatableMap(30000000);
//        for (int j = 0; j < 20; j++) {
            for(int i = 0 ; i < 30000000; i ++){
                map1.put(random.nextInt(Integer.MAX_VALUE),i);
            }
//        }
//        HashIntIntMap map2 = HashIntIntMaps.newUpdatableMap();
////        for (int j = 0; j < 20; j++) {
//            for(int i = 0 ; i < 5000000; i ++){
//                map2.addValue(random.nextInt(Integer.MAX_VALUE),i);
//            }
////        }

        long t2 = System.currentTimeMillis();

        IntArrayList intList = new IntArrayList(30000000);
        for(int i = 0 ; i < 30000000; i ++){
            intList.add(random.nextInt(Integer.MAX_VALUE));
            intList.add(1);
        }

        long t3 = System.currentTimeMillis();

        ArrayList<Integer> arrayList = new ArrayList(30000000  *2);
        for(int i = 0 ; i < 30000000; i ++){
            arrayList.add(random.nextInt(Integer.MAX_VALUE));
            arrayList.add(1);
        }

        long t4 = System.currentTimeMillis();

//        final int[] all = {0};
//        map2.cursor().forEachForward((k,v)-> all[0]++);
//        for (Map.Entry<Integer, Integer>
//             entry : map2.entrySet()) {
//            map1.addValue(entry.getKey(), entry.getValue());
//        }

//        IntListIterator iterator = intList.iterator();
//        while (iterator.hasNext()){
//            map1.addValue(iterator.nextInt(),iterator.nextInt());
//        }

        System.out.println((t3-t2) + "," + (t2-t1) + "," + (t4-t3));
        SizeOfUtil.printSize(map1);
        SizeOfUtil.printSize(intList);
        SizeOfUtil.printSize(arrayList);



    }

    public static void main(String[] args) {
        BitSet bitSet = new BitSet(Integer.MAX_VALUE / 2);
        bitSet.set(1);
        SizeOfUtil.printSize(bitSet);
        System.out.println(bitSet.get(1) + "," + bitSet.get(0));

        int dataSize = 67544354;
        HashIntIntMap map1 = HashIntIntMaps.newUpdatableMap(dataSize);
        Random random = new Random();
//        for (int j = 0; j < 20; j++) {
//        long t11 = System.currentTimeMillis();
//        for(int i = 0 ; i < dataSize; i ++){
//            int v = random.nextInt(Integer.MAX_VALUE);
//            map1.put(v,i);
//        }

        long t12 = System.currentTimeMillis();

        for(int i = 0 ; i < dataSize; i ++){
            int v = random.nextInt(Integer.MAX_VALUE);
            bitSet.set(v);
        }
        long t13 = System.currentTimeMillis();

        bitSet.toByteArray();

        long t14 = System.currentTimeMillis();

        System.out.println(t14-t13);

//        int size = 100000000;
//
//        long t1 = System.currentTimeMillis();
//
//        for (int i = 0;i < size; i++){
//            map1.containsKey(i);
//        }
//
//        long t2 = System.currentTimeMillis();
//
//        for (int i = 0;i < size; i++){
//            bitSet.get(i);
//        }
//
//        long t3 = System.currentTimeMillis();
//
//        System.out.println((t2-t1) + "," + (t3-t2) + "," + (t13-t12) + "," + (t12-t11));
//        SizeOfUtil.printSize(map1);



    }
}
