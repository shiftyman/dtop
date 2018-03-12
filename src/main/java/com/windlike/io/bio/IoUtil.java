package com.windlike.io.bio;

import com.koloboke.collect.IntIterator;
import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.set.hash.HashIntSet;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Created by windlike.xu on 2018/3/6.
 */
public class IoUtil {


    public static void readQueryFirstOrder2(DataInputStream input, HashIntSet userIdSet, int expertSize) throws IOException {
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
                    userIdSet.add((((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0));
                    readTime ++;
                }
                off = 0;
//                        lineLength = 0;
            }
        }while (length > 0);

        if(off > 0){
            for(int i = 0; i < off; i+=4){
                userIdSet.add((((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0));
                readTime ++;
            }
        }

        if(readTime != expertSize){//// TODO: 2018/3/6
            throw new RuntimeException("readQueryFirstOrder 读取的size有问题！read：" + readTime + ",expertSize:" + expertSize);
        }
    }

    public static void readQueryFirstOrder3(DataInputStream input, BitSet bitSet, int expertSize) throws IOException {
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
                    bitSet.set((((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0));
                    readTime ++;
                }
                off = 0;
//                        lineLength = 0;
            }
        }while (length > 0);

        if(off > 0){
            for(int i = 0; i < off; i+=4){
                bitSet.set((((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0));
                readTime ++;
            }
        }

        if(readTime != expertSize){//// TODO: 2018/3/6
            throw new RuntimeException("readQueryFirstOrder 读取的size有问题！read：" + readTime + ",expertSize:" + expertSize);
        }
    }

    public static BitSet readQueryFirstOrder(DataInputStream input, int size) throws IOException {
        long[] longArray = new long[size];
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int length;
        int off = 0;
        int readTime = 0;
        do{
            length = input.read(bytes, off, bufsize - off);
            off += length;
            if(off == bufsize){
                //读取
                for(int i = 0; i < bufsize; i+=8){
                    longArray[readTime] = (((long)bytes[i] << 56) +
                            ((long)(bytes[i+1] & 255) << 48) +
                            ((long)(bytes[i+2] & 255) << 40) +
                            ((long)(bytes[i+3] & 255) << 32) +
                            ((long)(bytes[i+4] & 255) << 24) +
                            ((bytes[i+5] & 255) << 16) +
                            ((bytes[i+6] & 255) <<  8) +
                            ((bytes[i+7] & 255) <<  0));

                    readTime ++;
                }
                off = 0;
            }
        }while (length > 0);

        if(off > 0){
            for(int i = 0; i < off; i+=8){
                longArray[readTime] = (((long)bytes[i] << 56) +
                        ((long)(bytes[i+1] & 255) << 48) +
                        ((long)(bytes[i+2] & 255) << 40) +
                        ((long)(bytes[i+3] & 255) << 32) +
                        ((long)(bytes[i+4] & 255) << 24) +
                        ((bytes[i+5] & 255) << 16) +
                        ((bytes[i+6] & 255) <<  8) +
                        ((bytes[i+7] & 255) <<  0));
                readTime ++;
            }
        }

        if(readTime != size){//// TODO: 2018/3/6
            throw new RuntimeException("readQueryFirstOrder 读取的size有问题！read：" + readTime + ",expertSize:" + size);
        }

        return BitSet.valueOf(longArray);
    }

    public static void writeQueryFirstOrder2(DataOutputStream output, HashIntSet userIdSet) throws IOException {
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int v;
        int off = 0;
        IntIterator iter = userIdSet.iterator();
        while(iter.hasNext()){
            v = iter.nextInt();
            bytes[off] = (byte) ((v >>> 24) & 0xFF);
            bytes[off + 1] = (byte) ((v >>> 16) & 0xFF);
            bytes[off + 2] = (byte) ((v >>> 8) & 0xFF);
            bytes[off + 3] = (byte) ((v >>> 0) & 0xFF);
            off+=4;
            if(off == bufsize){
                output.write(bytes);
                off=0;
//                writeTime++;
            }
        }
        if(off > 0){
            output.write(bytes, 0, off);
        }

    }

    public static void writeQueryFirstOrder(DataOutputStream output, BitSet userIdBitSet) throws IOException {
//        byte[] userIdBytes = userIdBitSet.toByteArray();
        long[] longArray = userIdBitSet.toLongArray();
        output.writeInt(longArray.length);//size
        long v = 0;
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int off = 0;
        for(int i = 0 ; i < longArray.length; i++){
            v = longArray[i];
            bytes[off]     = (byte)(v >>> 56);
            bytes[off + 1] = (byte)(v >>> 48);
            bytes[off + 2] = (byte)(v >>> 40);
            bytes[off + 3] = (byte)(v >>> 32);
            bytes[off + 4] = (byte)(v >>> 24);
            bytes[off + 5] = (byte)(v >>> 16);
            bytes[off + 6] = (byte)(v >>>  8);
            bytes[off + 7] = (byte)(v >>>  0);

            off+=8;
            if(off == bufsize){
                output.write(bytes);
                off=0;
            }
        }

        if(off > 0){
            output.write(bytes, 0, off);
        }

//        int off = 0;//读取偏移量
//        int maxSizeForOneWrite = 1024;
//        int leftBytes = userIdBytes.length;//剩余字节数
//        while (leftBytes > 0){
//            int writeLength = leftBytes < maxSizeForOneWrite ? leftBytes:maxSizeForOneWrite;
//            output.write(userIdBytes, off, writeLength);
//            off+=writeLength;
//            leftBytes-=writeLength;
//        }
    }

    public static void readTopNUser2(DataInputStream input, HashIntIntMap userNumMap, int expertSize) throws IOException {
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int length;
        int off = 0;
        long readTime = 0;
        int partnerNum = 0;
        int[] data = new int[2];

        int leftBytes = expertSize * 8;


        do{
            int len = bufsize - off;
            length = input.read(bytes, off, len < leftBytes ? len : leftBytes);
            off += length;
            leftBytes -= length;
            if(off == bufsize || leftBytes == 0){
//                        off = lineLength;
//                    }else{
                //读取
                for(int i = 0; i < off; i+=4){
                    data[partnerNum] =  (((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0);
                    partnerNum++;
                    if(partnerNum == 2){//一对
                        readTime ++;

                        userNumMap.put(data[0], data[1]);
                        partnerNum = 0;
//                        input.readInt();
                    }

                }
                off = 0;
//                        lineLength = 0;
            }
        }while (length > 0);

        if(readTime != expertSize){//// TODO: 2018/3/6
            throw new RuntimeException("readTopNUser 读取的size有问题！read：" + readTime + ",expertSize:" + expertSize);
        }
    }

    public static void readTopNUser(DataInputStream input, List<Integer> userNumList, int expertSize) throws IOException {
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int length;
        int off = 0;
        long readTime = 0;
        int leftBytes = expertSize * 8;


        do{
            int len = bufsize - off;
            length = input.read(bytes, off, len < leftBytes ? len : leftBytes);
            off += length;
            leftBytes -= length;
            if(off == bufsize || leftBytes == 0){
                //读取
                for(int i = 0; i < off; i+=4){
                    userNumList.add((((bytes[i] & 0x000000FF) << 24)) + ((bytes[i+1] & 0x000000FF ) << 16) + ((bytes[i+2]& 0x000000FF) << 8) + ((bytes[i+3]& 0x000000FF) << 0));
                    readTime ++;
                }
                off = 0;
            }
        }while (length > 0);

        readTime = readTime / 2;
        if(readTime != expertSize){//// TODO: 2018/3/6
            throw new RuntimeException("readTopNUser 读取的size有问题！read：" + readTime + ",expertSize:" + expertSize);
        }
    }

    public static void main(String[] args) {
//        int v = 493654074;
//        byte[] bytes = new byte[4];
//        int off = 0;
//        bytes[off] = (byte) ((v >>> 24) & 0xFF);
//        bytes[off + 1] = (byte) ((v >>> 16) & 0xFF);
//        bytes[off + 2] = (byte) ((v >>> 8) & 0xFF);
//        bytes[off + 3] = (byte) ((v >>> 0) & 0xFF);
//
//        byte b1 = 29;
//        byte b2 = 108;
//        byte b3 = -112;
//        byte b4 = 58;
//
//        int value = 0;
//        for (int i = 0; i < 4; i++) {
//            int shift = (4 - 1 - i) * 8;
//            value += (bytes[i] & 0x000000FF) << shift;// 往高位游
//        }
//
//        System.out.println(value);

//        ByteBuffer buffer = ByteBuffer.allocate(8);
//        buffer.putLong(0, 80767232323232322L);
        long v = 80767232323232322L;

        byte[] bytes = new byte[8];
        bytes[0]     = (byte)(v >>> 56);
        bytes[0 + 1] = (byte)(v >>> 48);
        bytes[0 + 2] = (byte)(v >>> 40);
        bytes[0 + 3] = (byte)(v >>> 32);
        bytes[0 + 4] = (byte)(v >>> 24);
        bytes[0 + 5] = (byte)(v >>> 16);
        bytes[0 + 6] = (byte)(v >>>  8);
        bytes[0 + 7] = (byte)(v >>>  0);

        long v2 = (((long)bytes[0] << 56) +
                ((long)(bytes[1] & 255) << 48) +
                ((long)(bytes[2] & 255) << 40) +
                ((long)(bytes[3] & 255) << 32) +
                ((long)(bytes[4] & 255) << 24) +
                ((bytes[5] & 255) << 16) +
                ((bytes[6] & 255) <<  8) +
                ((bytes[7] & 255) <<  0));
        System.out.println( "," + v2 + "," + v);
        System.out.println(v2==v);

    }

    public static void writeTopNUser(DataOutputStream output, HashIntIntMap userMap) throws IOException {
        int bufsize = 1024;
        byte[] bytes = new byte[bufsize];
        int off = 0;
        for(Map.Entry<Integer, Integer> entry: userMap.entrySet()){
            int v = entry.getKey();
            bytes[off] = (byte) ((v >>> 24) & 0xFF);
            bytes[off + 1] = (byte) ((v >>> 16) & 0xFF);
            bytes[off + 2] = (byte) ((v >>> 8) & 0xFF);
            bytes[off + 3] = (byte) ((v >>> 0) & 0xFF);
            v = entry.getValue();
            bytes[off + 4] = (byte) ((v >>> 24) & 0xFF);
            bytes[off + 5] = (byte) ((v >>> 16) & 0xFF);
            bytes[off + 6] = (byte) ((v >>> 8) & 0xFF);
            bytes[off + 7] = (byte) ((v >>> 0) & 0xFF);
            off+=8;
            if(off == bufsize){
                output.write(bytes);
                off=0;
            }
        }
        if(off > 0){
            output.write(bytes, 0, off);
        }
    }
}
