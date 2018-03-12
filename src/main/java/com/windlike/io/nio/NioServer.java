package com.windlike.io.nio;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class NioServer {

    private int port;
    private String[] args;

    private boolean finished = false;

    private ServerSocketChannel serverChannel;
    private ExecutorService workers = Executors.newCachedThreadPool();

    public NioServer(int port) {
        this.port = port;
        try {
            this.serverChannel = ServerSocketChannel.open();
            serverChannel.configureBlocking(true);
            serverChannel.socket().bind(new InetSocketAddress(port));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    try {
                        SocketChannel clientChannel = serverChannel.accept();
                        clientChannel.setOption(StandardSocketOptions.SO_KEEPALIVE, true);
                        clientChannel.configureBlocking(true);
                        workers.execute(new ServerWorker(clientChannel));
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }, "nio.NioServer-listening-thread").start();
    }

//    public void send(ByteBuffer data) {
//        try {
//            sendQueue.put(data);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

//    public void finish() {
//        try {
//            sendQueue.put(ByteBuffer.wrap("F".getBytes()));
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        try {
//            while (!finished) {
//                TimeUnit.MILLISECONDS.sleep(100);
//            }
//            serverThreadsPool.shutdown();
//            serverThreadsPool.awaitTermination(1000, TimeUnit.MILLISECONDS);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }

    public static void main(String[] args) {
        new NioServer(8080).start();
//
        while (true){
            try {
                Thread.currentThread().sleep(100000000000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
