package com.windlike.io;

import com.windlike.io.bio.Server;

/**
 * Created by windlike.xu on 2018/3/5.
 */
public class SlaveApp {

    public static void main(String[] args) {
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
