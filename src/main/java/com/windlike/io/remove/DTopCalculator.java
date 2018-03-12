//package com.windlike.io;
//
//import com.koloboke.collect.map.hash.HashLongObjMap;
//import com.koloboke.collect.map.hash.HashLongObjMaps;
//import com.windlike.io.vo.ActivityVo;
//
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.LinkedList;
//import java.util.List;
//
///**
// * Created by windlike.xu on 2018/2/26.
// */
//public class DTopCalculator {
//
//    private int k = 2;//top k
//
//    private int machine = 3;//机器数量
//
//    private HashLongObjMap<ActivityVo> hashLongObjMap = HashLongObjMaps.newUpdatableMap(k * 2);
//
//    private List<ActivityVo> data1 = new LinkedList<>();
//    private List<ActivityVo> data2 = new LinkedList<>();
//    private List<ActivityVo> data3 = new LinkedList<>();
//
////    {
////        data1.add(new ActivityVo(1, 1, 1, 60, 1));
////        data1.add(new ActivityVo(2, 1, 1, 3, 1));
////        data1.add(new ActivityVo(3, 1, 1, 55, 1));
////
////        data2.add(new ActivityVo(1, 1, 1, 3, 1));
////        data2.add(new ActivityVo(2, 1, 1, 60, 1));
////        data2.add(new ActivityVo(3, 1, 1, 55, 1));
////
////        data3.add(new ActivityVo(1, 1, 1, 30, 1));
////        data3.add(new ActivityVo(2, 1, 1, 3, 1));
////        data3.add(new ActivityVo(3, 1, 1, 29, 1));
////    }
//
//    public DTopCalculator(List<ActivityVo> data1, List<ActivityVo> data2, List<ActivityVo> data3){
//        this.data1 = data1;
//        this.data2 = data2;
//        this.data3 = data3;
//    }
//
//    public List<ActivityVo> calculateTopK(List<ActivityVo> t1, List<ActivityVo> t2, List<ActivityVo> t3, int time){
//        //merge
//        if(t1 != null){
//            mergeToMap(t1);
//        }
//        if(t2 != null){
//            mergeToMap(t2);
//        }
//        if(t3 != null){
//            mergeToMap(t3);
//        }
//
//        //replenish
//        List<Long> waitQuerys = new LinkedList<>();
//        hashLongObjMap.values().forEach((o)->{
//           if (o.getMergeNum() < machine){
//                //查询各机器
//               waitQuerys.add(o.getActPlatfrom());
//           }
//        });
//        //// TODO: 2018/2/28 查询
//        if(waitQuerys.size() > 0){
//            mergeToMap(queryMore(waitQuerys, data1));
//            mergeToMap(queryMore(waitQuerys, data2));
//            mergeToMap(queryMore(waitQuerys, data3));
//        }
//
//        //sort
//        List<ActivityVo> list = new ArrayList<>(hashLongObjMap.size());
//        hashLongObjMap.values().forEach((o) -> list.add(o));
//        Collections.reverse(list);
//
//        if(time == 1){//若第一次调用，还需要额外一次计算做精化
//            //make accurate
//            ActivityVo brandLocalVo = list.get(k - 1);
//
//            int p = brandLocalVo.getAllNum();
//            int m = machine;
//            double threshold;
//
//            byte deleteMachine = 0;
//            while(true){
//                int deleteNum = 0;
//                threshold = p / m;//计算阈值
//
//                if((deleteMachine & 1) == 0 && t1.get(k-1).getAllNum() <= threshold){
//                    deleteNum++;
//                    deleteMachine |= 1;
//                    m -= 1;
//                    p -= t1.get(k-1).getAllNum();
//                }
//                if((deleteMachine & 2) == 0 && t2.get(k-1).getAllNum() <= threshold){
//                    deleteNum++;
//                    deleteMachine |= 2;
//                    m -= 1;
//                    p -= t2.get(k-1).getAllNum();
//                }
//                if((deleteMachine & 4) == 0 && t3.get(k-1).getAllNum() <= threshold){
//                    deleteNum++;
//                    deleteMachine |= 4;
//                    m -= 1;
//                    p -= t3.get(k-1).getAllNum();
//                }
//
//                if(deleteNum == 0){
//                    break;
//                }
//            }
//
//            //send threshold to all machine
//            if((deleteMachine & 1) == 0){
//                //// TODO: 2018/2/28 send to m1
//                t1 = queryGtThreshold(threshold, data1);
//            }else{
//                t1 = null;
//            }
//            if((deleteMachine & 2) == 0){
//                //// TODO: 2018/2/28 send to m2
//                t2 = queryGtThreshold(threshold, data2);
//            }else{
//                t2 = null;
//            }
//            if((deleteMachine & 4) == 0){
//                //// TODO: 2018/2/28 send to m3
//                t3 = queryGtThreshold(threshold, data3);
//            }else{
//                t3 = null;
//            }
//
//            //merge & replenish
//            //sort and return
//            return calculateTopK(t1, t2, t3, 2);
//        }else{
//            return list;
//        }
//    }
//
//    private void mergeToMap(List<ActivityVo> list){
//        for(ActivityVo vo : list){
//            ActivityVo old = hashLongObjMap.get(vo.getActPlatfrom());
//            if(old != null){
//                old.merge(vo);
//            }else{
//                hashLongObjMap.put(vo.getActPlatfrom(), new ActivityVo(vo));
//            }
//        }
//    }
//
//    public List<ActivityVo> queryMore(List<Long> actPlaformList, List<ActivityVo> data){
//        List<ActivityVo> result = new LinkedList<>();
//        for (long actPlatform : actPlaformList){
//            for (ActivityVo vo : data){
//                if(actPlatform == vo.getActPlatfrom() && !vo.isHasSent()){
//                    result.add(vo);
//                    vo.setHasSent(true);
//                }
//            }
//        }
//        return result;
//    }
//
//    public List<ActivityVo> queryGtThreshold(double threshold, List<ActivityVo> data){
//        List<ActivityVo> result = new LinkedList<>();
//        for (ActivityVo vo : data){
//            if(!vo.isHasSent() && vo.getAllNum() > threshold){
//                result.add(vo);
//                vo.setHasSent(true);
//            }
//        }
//        return result;
//    }
//
//    public static void main(String[] args) {
//        List<ActivityVo> data1 = new LinkedList<>();
//        List<ActivityVo> data2 = new LinkedList<>();
//        List<ActivityVo> data3 = new LinkedList<>();
//
//        data1.add(new ActivityVo(1, 1, 1, 60, 1));
//        data1.add(new ActivityVo(2, 1, 1, 3, 1));
//        data1.add(new ActivityVo(3, 1, 1, 55, 1));
//
//        data2.add(new ActivityVo(1, 1, 1, 3, 1));
//        data2.add(new ActivityVo(2, 1, 1, 31, 1));
//        data2.add(new ActivityVo(3, 1, 1, 20, 1));
//
//        data3.add(new ActivityVo(1, 1, 1, 30, 1));
//        data3.add(new ActivityVo(2, 1, 1, 3, 1));
//        data3.add(new ActivityVo(3, 1, 1, 29, 1));
//
//        DTopCalculator dTopCalculator = new DTopCalculator(data1, data2, data3);
//        List<ActivityVo> t1 = new ArrayList<>(1);
//        t1.add(data1.get(0));
//        t1.add(data1.get(2));
//        data1.get(0).setHasSent(true);
//        data1.get(2).setHasSent(true);
//
//        List<ActivityVo> t2 = new ArrayList<>(1);
//        t2.add(data2.get(1));
//        t2.add(data2.get(2));
//        data2.get(1).setHasSent(true);
//        data2.get(2).setHasSent(true);
//
//        List<ActivityVo> t3 = new ArrayList<>(1);
//        t3.add(data3.get(0));
//        t3.add(data3.get(2));
//        data3.get(0).setHasSent(true);
//        data3.get(2).setHasSent(true);
//
//        dTopCalculator.calculateTopK(t1, t2, t3, 1).forEach((o)-> System.out.println(o.getActPlatfrom() + "," + o.getAllNum()));
//    }
//}
