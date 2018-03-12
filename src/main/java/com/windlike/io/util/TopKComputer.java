package com.windlike.io.util;

import com.koloboke.collect.map.hash.HashIntIntMap;
import com.koloboke.collect.map.hash.HashIntIntMaps;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.vo.ActivityVo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by windlike.xu on 2018/3/3.
 */
public class TopKComputer {

    public static ActivityVo[] computeTopK(List<ActivityVo> activityList, int k){

        ActivityVo[] hval = new ActivityVo[k];
        int i = 0;
        MinHeap<ActivityVo> heap = null;
        for (ActivityVo activityVo : activityList){
            if(i < k){//前40个元素，先建堆
                hval[i] = activityVo;
                if(i == k - 1){
                    heap = new MinHeap<>(hval);//建堆
                }
                i++;//有效才++
            }else{
                ActivityVo root = heap.getRoot();

                // 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
                if(activityVo.compareTo(root) > 0)//相等也舍弃，因为最后多路归并后末尾元素还存在且有连续排位相同的概率很低
                {
                    //有效数据才有资格进入堆中
                    heap.setRoot(activityVo);
                    heap.heapify(0);
                }
            }
        }

        ActivityVo[] result = new ActivityVo[k];
        int j = k-1;
        ActivityVo min;
        while((min = heap.removeMin()) != null){
            result[j] = min;
            j--;
        }

        return result;
    }

    public static ActivityVo[] computeTopKAfterMerge(HashLongObjMap<ActivityVo> activityMap, int k){

        ActivityVo[] hval = new ActivityVo[k];
        int i = 0;
        MinHeap<ActivityVo> heap = null;
        for (ActivityVo activityVo : activityMap.values()){
            if(activityVo.getMergeNum() == Constants.MACHINE){
                if(i < k){//前40个元素，先建堆
                    hval[i] = activityVo;
                    if(i == k - 1){
                        heap = new MinHeap<>(hval);//建堆
                    }
                    i++;//有效才++
                }else{
                    ActivityVo root = heap.getRoot();

                    // 当数据大于堆中最小的数（根节点）时，替换堆中的根节点，再转换成堆
                    if(activityVo.compareTo(root) > 0)//相等也舍弃，因为最后多路归并后末尾元素还存在且有连续排位相同的概率很低
                    {
                        //有效数据才有资格进入堆中
                        heap.setRoot(activityVo);
                        heap.heapify(0);
                    }
                }
            }
        }

        ActivityVo[] result = new ActivityVo[k];
        int j = k-1;
        ActivityVo min;
        while((min = heap.removeMin()) != null){
            result[j] = min;
            j--;
        }

        return result;
    }

    public static List<ActivityVo> compressActivityMap(List<ActivityVo> activityList){
        ActivityVo[] activityVos = TopKComputer.computeTopK(activityList, Constants.VALID_CANDIDATE_NUM);
//        HashLongObjMap<ActivityVo> s2ActivityMap = HashLongObjMaps.newUpdatableMap(Constants.VALID_CANDIDATE_NUM);
//        for (ActivityVo activity : activityVos) {
//            s2ActivityMap.put(activity.getActPlatfrom(), activity);
//        }

        for(int i = 0 ; i < activityVos.length; i++){
//            int supposeSize = 0;
//            if(i == 1){
//                supposeSize = 51000000;
//            }else if(i < 5){
//                supposeSize = 4000000;
//            }else if(i < 10){
//                supposeSize = 3300000;
//            }else if(i < 18){
//                supposeSize = 2500000;
//            }else if(i < 25){
//                supposeSize = 2100000;
//            }else if(i < 35){
//                supposeSize = 1900000;
//            }else if(i < 35){
//                supposeSize = 1840000;
//            }else if(i < 40){
//                supposeSize = 1500000;
//            }else if(i < 50){
//                supposeSize = 950000;
//            }else if(i < 60){
//                supposeSize = 850000;
//            }else if(i < 70){
//                supposeSize = 750000;
//            }else if(i < 80){
//                supposeSize = 600000;
//            }else if(i < 90){
//                supposeSize = 450000;
//            }else if(i < 100){
//                supposeSize = 490000;
//            }else{
//                supposeSize = 450000;
//            }


            ActivityVo old = activityVos[i];
            HashIntIntMap newUserNumMap = HashIntIntMaps.newUpdatableMap(old.getUserNumMap().size() * 100 + 50000);
            newUserNumMap.putAll(old.getUserNumMap());
            old.setUserNumMap(newUserNumMap);
        }

        activityList = new ArrayList<>(activityVos.length);
        for(ActivityVo vo : activityVos){
            activityList.add(vo);
        }

        return activityList;
    }

    public static HashLongObjMap<HashLongSet> compressBrandMap(HashLongObjMap<HashLongSet> brandMap, List<ActivityVo> activityList){
        HashLongObjMap<HashLongSet> s2brandMap = HashLongObjMaps.newUpdatableMap(Constants.VALID_CANDIDATE_NUM * 50);
        brandMap.cursor().forEachForward((brandkey, brandActs)->{
            byte platform = (byte) (brandkey & 3);
            HashLongSet newBrandActs = HashLongSets.newUpdatableSet(24);

            //先保存老index和新index的关系
            HashIntIntMap indexMap = HashIntIntMaps.newUpdatableMap(activityList.size());
            for(int i = 0; i < activityList.size(); i++){
                indexMap.put(activityList.get(i).getIndex(), i);
            }

            brandActs.cursor().forEachForward((x) -> {
//                long actPlatform = ((actStartEnd / 10000 + 20170000000000000L) << 2) + platform;
//                if(activityList.get(actPlatform) != null){
//                    newBrandActs.add(actStartEnd);
//                }
                int index = (int) (x & 0xffff);
                long endMonDay = ((x-index) >>> 16) & 0xffff;
                long startMonDay = x >>> 32;
                for(int i = 0; i < activityList.size(); i++){
                    ActivityVo vo = activityList.get(i);
                    if(vo.getIndex() == index){
                        long brandAct = (startMonDay << 32) + (endMonDay << 16) + indexMap.get(index);//老index->newIndex
                        newBrandActs.add(brandAct);
                        break;
                    }
                }
            });

            if(newBrandActs.size() > 0){
                s2brandMap.put(brandkey, newBrandActs);
            }
        });

        //更新activitys的index
        for(int i = 0; i < activityList.size(); i++){
            activityList.get(i).setIndex(i);
        }

        return s2brandMap;
    }

    private static String printInfo(List<ActivityVo> activityList){
        long allNum = 0;
        long userNum = 0;
        for (ActivityVo activityVo : activityList){
            allNum += activityVo.getAllNum();
            userNum += activityVo.getUserNumMap().size();
        }
        return "allNum:" + allNum + ",userNum:" + userNum;
    }

    public static void collectInfoAndPrint(List<ActivityVo> activityList){
        ActivityVo[] result = TopKComputer.computeTopK(activityList, 40);
        Random random = new Random();
        String filename = random.nextInt(50) + ".tt";
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(filename))){
            System.out.println(printInfo(activityList) + ", save to" + filename);//sout
            fileWriter.write(printInfo(activityList) + "\n");
            for (ActivityVo vo : result
                    ) {
                fileWriter.write(vo + "\n");
            }
            fileWriter.flush();
            fileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String printBrandActLinkSum(HashLongObjMap<HashLongSet> brandMap){
        int sum = 0;
        for (HashLongSet brandLink : brandMap.values()){
            sum += brandLink.size();
        }
        return "all brand link sum = " + sum;
    }
}
