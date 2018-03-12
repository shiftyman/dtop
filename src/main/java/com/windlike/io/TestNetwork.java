package com.windlike.io;

import com.windlike.io.bio.Client;
import com.windlike.io.bio.Protocol;
import org.omg.Messaging.SYNC_WITH_TRANSPORT;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Created by windlike.xu on 2018/3/6.
 */
public class TestNetwork {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        long t1 = System.currentTimeMillis();

        int size = Integer.parseInt(args[0]);
        int thread = Integer.parseInt(args[1]);
        ExecutorService pool = Executors.newFixedThreadPool(thread);
        List<Future> futureList = new LinkedList<>();
        for (int i =0 ; i < thread; i ++){
            futureList.add(pool.submit(()->{
                boolean msgSendSuccessd = false;
                do{
                    try {
                        new Client(Constants.SLAVE_IP_1, Constants.SERVER_LISTENING_PORT).send(Protocol.MsgType.TEST, size / thread);
                        msgSendSuccessd = true;
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                } while (!msgSendSuccessd);
            }));
        }


        for (Future future:
             futureList) {
            future.get();
        }
        long t3 = System.currentTimeMillis();
        System.out.println(t3-t1);
    }
}
