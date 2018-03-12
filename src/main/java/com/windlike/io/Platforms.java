package com.windlike.io;

/**
 * Created by windlike.xu on 2018/3/1.
 */
public class Platforms {

//    APP("app","p",0),
//    WAP("wap","a",1),
//    PC("pc","c",2),
//    WEIXIN("weixin","e",3);

    //private String[] plantforms = new String[]{"app", "wap", "pc", "weixin"};
//    private String platform;
//
//    private String shortName;
//
//    private int index;
//
//
//    Platforms(String platform, String shortName, int index) {
//        this.platform = platform;
//        this.shortName = shortName;
//        this.index = index;
//    }

    public static byte shortNameToIndex(char c){
        if(c == 'p'){
            return 0;
        }else if(c == 'a'){
            return 1;
        }else if(c == 'c'){
            return 2;
        }else if(c == 'e'){
            return 3;
        }else{
            throw new IllegalArgumentException();
        }
    }

    public static String indexToFullName(byte index){
        if(index == 0){
            return "app";
        }else if(index == 1){
            return "wap";
        }else if(index == 2){
            return "pc";
        }else if(index == 3){
            return "weixin";
        }else{
            throw new IllegalArgumentException();
        }
    }

    public static int indexToLeftCharNum(byte index){
        if(index == 0){
            return 2;
        }else if(index == 1){
            return 2;
        }else if(index == 2){
            return 1;
        }else if(index == 3){
            return 5;
        }else{
            throw new IllegalArgumentException();
        }
    }
}
