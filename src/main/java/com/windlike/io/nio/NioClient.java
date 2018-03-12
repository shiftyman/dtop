package com.windlike.io.nio;

import com.windlike.io.Constants;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class NioClient {

    private String hostName;
    private int port;
    private SocketChannel clientChannel;

    public NioClient(String hostName, int port){
        this.hostName = hostName;
        this.port = port;
        establishConnection();
    }

    private void establishConnection(){
        while(true) {
            try {
                clientChannel = SocketChannel.open();
                clientChannel.configureBlocking(true);
                clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                clientChannel.connect(new InetSocketAddress(hostName, port));
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                TimeUnit.MILLISECONDS.sleep(500);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public void start(int size){
        try {
            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1);

            for(int i = 0; i < size;i++){
                byteBuffer.clear();
                byteBuffer.put((byte) 1);
                byteBuffer.flip();
                while(byteBuffer.hasRemaining()) {//确保完全写入
                    clientChannel.write(byteBuffer);
                }
            }
//
//
//
//            ByteBuffer byteBuffer = ByteBuffer.allocateDirect(1024);
//            byteBuffer.clear();
//            byteBuffer.put("I am Tom. How are you?".getBytes());
//            byteBuffer.flip();
//            clientChannel.write(byteBuffer);
//            while(byteBuffer.hasRemaining()) {//确保完全写入
//                clientChannel.write(byteBuffer);
//            }
//
//            Thread.sleep(20000L);
//            System.out.println("睡醒");
//
//            byteBuffer.clear();
//            byteBuffer.put("How do you do?".getBytes());
//            byteBuffer.flip();
//            clientChannel.write(byteBuffer);
//            while(byteBuffer.hasRemaining()) {//确保完全写入
//                clientChannel.write(byteBuffer);
//            }
//
//            System.out.println("完毕，关闭");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void closeConnection(){
        try {
            clientChannel.finishConnect();
            clientChannel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void main(String[] args) {
        new NioClient(Constants.SLAVE_IP_1, 8080).start(Integer.parseInt(args[0]));
    }
}
