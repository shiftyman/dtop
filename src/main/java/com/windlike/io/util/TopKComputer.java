package com.windlike.io.util;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.vo.ActivityVo;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

/**
 * Created by windlike.xu on 2018/3/3.
 */
public class TopKComputer {

    public static ActivityVo[] computeTopK(HashLongObjMap<ActivityVo> activityMap, int k){

        ActivityVo[] hval = new ActivityVo[k];
        int i = 0;
        MinHeap<ActivityVo> heap = null;
        for (ActivityVo activityVo : activityMap.values()){
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

    public static HashLongObjMap<ActivityVo> compressActivityMap(HashLongObjMap<ActivityVo> activityMap){
        ActivityVo[] activityVos = TopKComputer.computeTopK(activityMap, Constants.VALID_CANDIDATE_NUM);
        HashLongObjMap<ActivityVo> s2ActivityMap = HashLongObjMaps.newUpdatableMap(Constants.VALID_CANDIDATE_NUM * 2);
        for (ActivityVo activity : activityVos) {
            s2ActivityMap.put(activity.getActPlatfrom(), activity);
        }
        return s2ActivityMap;
    }

    public static HashLongObjMap<HashLongSet> compressBrandMap(HashLongObjMap<HashLongSet> brandMap, HashLongObjMap<ActivityVo> activityMap){
        HashLongObjMap<HashLongSet> s2brandMap = HashLongObjMaps.newUpdatableMap(Constants.VALID_CANDIDATE_NUM * 50);
        brandMap.cursor().forEachForward((brandkey, brandActs)->{
            byte platform = (byte) (brandkey & 3);
            HashLongSet newBrandActs = HashLongSets.newUpdatableSet(16);
            brandActs.cursor().forEachForward((actStartEnd) -> {
                long actPlatform = ((actStartEnd / 10000 + 20170000000000000L) << 2) + platform;
                if(activityMap.get(actPlatform) != null){
                    newBrandActs.add(actStartEnd);
                }
            });

            if(newBrandActs.size() > 0){
                s2brandMap.put(brandkey, newBrandActs);
            }
        });

        return s2brandMap;
    }

    private static String printInfo(HashLongObjMap<ActivityVo> activityMap){
        long allNum = 0;
        long userNum = 0;
        for (ActivityVo activityVo : activityMap.values()){
            allNum += activityVo.getAllNum();
            userNum += activityVo.getUserNumMap().size();
        }
        return "allNum:" + allNum + ",userNum:" + userNum;
    }

    public static void collectInfoAndPrint(HashLongObjMap<ActivityVo> activityMap){
        ActivityVo[] result = TopKComputer.computeTopK(activityMap, 40);
        Random random = new Random();
        String filename = random.nextInt(50) + ".tt";
        try (BufferedWriter fileWriter = new BufferedWriter(new FileWriter(filename))){
            System.out.println(printInfo(activityMap) + ", save to" + filename);//sout
            fileWriter.write(printInfo(activityMap) + "\n");
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
