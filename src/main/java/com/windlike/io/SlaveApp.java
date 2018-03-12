package com.windlike.io;

import com.windlike.io.bio.Server;

/**
 * Created by windlike.xu on 2018/3/5.
 */
public class SlaveApp {

    public static void main(String[] args) {
//        if(args.length > 3){
//            Constants.DEFAULT_LIKE_GOOD_THREAD_NUM = Integer.parseInt(args[0]);//收藏数据
//            Constants.DEFAULT_FIRST_ORDER_THREAED_NUM = Integer.parseInt(args[1]);//首单数据
//            Constants.FILE_BUFFER_SIZE = Integer.parseInt(args[2]);//io buffer size
//            Constants.VALID_CANDIDATE_NUM = Integer.parseInt(args[3]);//topN
//            System.out.println("change params.");
//        }

        System.out.println("App run in slave mode");

        Server server = new Server(Constants.SERVER_LISTENING_PORT);
        server.start();

        while (true) {
            try {
                Thread.sleep(Long.MAX_VALUE);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
