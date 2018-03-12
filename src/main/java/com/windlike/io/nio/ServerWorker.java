package com.windlike.io.nio;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Created by Administrator on 2018/2/20/020.
 */
public class ServerWorker implements Runnable {

    private SocketChannel socketChannel;

    public ServerWorker(SocketChannel socketChannel){
        this.socketChannel = socketChannel;
    }

    @Override
    public void run() {
        ByteBuffer readBuff = ByteBuffer.allocateDirect(1024);
        try {
            while(true){
                int size = socketChannel.read(readBuff);
                if(size == -1){//connection isclosed
                    System.out.println("连接已关闭。");
                    break;
                }

//                readBuff.flip();
//                byte[] buf = new byte[1024];
//                readBuff.get(buf, 0, size);
//                readBuff.clear();
//                System.out.println(new String(buf));
                readBuff.flip();
                for(int i = 0 ; i < size; i++){
                    readBuff.get();
                }
                readBuff.clear();
            }

//            if (readBuff.get(0) == REQUIRE_ARGS) {
//                ByteBuffer argsBuff = ByteBuffer.wrap(new ArgumentsPayloadBuilder(args).toString().getBytes());
//                chunkSize.clear();
////                        logger.info("data chunk size: " + argsBuff.limit());
//                chunkSize.putInt(argsBuff.limit());
//                chunkSize.flip();
//                clientChannel.write(chunkSize);
//                clientChannel.write(argsBuff);
//                while (true) {
//                    try {
//                        ByteBuffer data = sendQueue.take();
//                        if (data.limit() == 1 && data.get(0) == FINISHED_ALL) {
//                            clientChannel.finishConnect();
//                            clientChannel.close();
//                            finished = true;
//                            break;
//                        } else {
//                            chunkSize.clear();
////                                    logger.info("data chunk size: " + data.limit());
//                            chunkSize.putInt(data.limit());
//                            chunkSize.flip();
//                            clientChannel.write(chunkSize);
//                            clientChannel.write(data);
//                        }
//
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
////                                logger.info(e.getMessage());
//                    }
//                }
//            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
