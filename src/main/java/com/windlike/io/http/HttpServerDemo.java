package com.windlike.io.http;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import com.windlike.io.MasterApp;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

public class HttpServerDemo {

	public static void main(String[] args) {
		try {
			InetSocketAddress addr = new InetSocketAddress(8080);
			HttpServer server = HttpServer.create(addr, 0);

			server.createContext("/windlike", new MyHandler());
			server.setExecutor(Executors.newCachedThreadPool());
			server.start();
			System.out.println("Server is listening on port 8080");
		} catch (Exception e) {
			System.out.println("Server is run error:" + e);
		}
	}
}

class MyHandler implements HttpHandler {

	private static AtomicBoolean processing = new AtomicBoolean(false);

	public void handle(HttpExchange exchange) throws IOException {
		long t1 = System.currentTimeMillis();

//		System.out.println("请求到来.方法是：" + exchange.getRequestMethod());
		if(processing.compareAndSet(false, true)){
			try {
				String result = MasterApp.fire();
				System.out.println("返回结果:");
				System.out.println(result);
				exchange.sendResponseHeaders(200, result.length());
				OutputStream responseBody = exchange.getResponseBody();
				responseBody.write(result.getBytes());
				responseBody.close();

			} catch (Exception e) {
				e.printStackTrace();
			} finally {
				processing.compareAndSet(true, false);//完毕

				long t2 = System.currentTimeMillis();
				System.out.println("本次请求返回时间："+(t2-t1));
			}
		}else{
			System.out.println("ERROR!重复请求！");
			exchange.sendResponseHeaders(403, 0);
			OutputStream responseBody = exchange.getResponseBody();
			responseBody.write("你不要同一时间段请求两次！！！".getBytes());
			responseBody.close();
		}
	}
}
