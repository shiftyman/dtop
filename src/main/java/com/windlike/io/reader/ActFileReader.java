package com.windlike.io.reader;

import com.koloboke.collect.map.hash.HashLongLongMap;
import com.koloboke.collect.map.hash.HashLongLongMaps;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.Platforms;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.util.MinHeap;
import com.windlike.io.util.TopKComputer;
import com.windlike.io.vo.ActivityVo;
import it.unimi.dsi.fastutil.longs.LongArrayList;

import java.io.*;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * 文件读写预处理任务
 * @author windlike.xu
 *
 */
public class ActFileReader {

	private String orignFileFullName;

	private HashLongObjMap<ActivityVo> activityMap;

	private HashLongObjMap<HashLongSet> brandMap;//val为'MMddHHmmssSSS(actname)|MMdd(endtime)'的list


	private int row = 0;//行数

	public ActFileReader(String orignFileFullName, HashLongObjMap<ActivityVo> activityMap, HashLongObjMap<HashLongSet> brandMap) {
		this.orignFileFullName = orignFileFullName;
		this.activityMap = activityMap;
		this.brandMap = brandMap;//brand和act的数量相当

	}

	public void read() {
		//启动读写任务
		File file  = new File(orignFileFullName);
		try (FileInputStream orginFile = new FileInputStream(file)){
			MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());

			readAndProcess(inputBuffer);

			//清理
			activityMap = null;
			brandMap = null;

			System.out.println("此任务完成，解释行数：" + row);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void readAndProcess(MappedByteBuffer byteBuffer){
		byte[] fileLineCache = new byte[50];
		int length = 0;

		int segmentType = 0;
		ActivityVo activityVo = null;
		long actPlatform = 0;
		byte platform = 0;
		long actName = 0;
		int endMonDay = 0;
		while (byteBuffer.hasRemaining()){
			switch (segmentType){
				case 0://platform
					byteBuffer.position(byteBuffer.position() + 1);//移动一位
					platform = Platforms.shortNameToIndex((char) byteBuffer.get());
					actPlatform += platform;
					byteBuffer.position(byteBuffer.position() + Platforms.indexToLeftCharNum(platform));

					break;
				case 1://actname
					byteBuffer.get(fileLineCache, 0, 17);
//					length = getSegment(byteBuffer, fileLineCache);
					actName = InvincibleConvertUtil.byteArrayToLong(fileLineCache, 0 ,17);//yyyyMMddHHmmssSSS
					actPlatform += actName << 2;

					activityVo = activityMap.get(actPlatform);
					if(activityVo == null){//new
						activityVo = new ActivityVo();
						activityMap.put(actPlatform, activityVo);

						activityVo.setActPlatfrom(actPlatform);
						//actStartTime
						activityVo.setStartTime(InvincibleConvertUtil.byteArrayToInt(fileLineCache, 4, 8) * 1000000 + 100000);//10点开场
						//skip starttime
						byteBuffer.position(byteBuffer.position() + 26);
					}else{//old
						byteBuffer.position(byteBuffer.position() + 51);//50=21+20+10 brandname至少10个字符
						segmentType++;//跳过case2，直达case3
					}

					break;
				case 2://act_end_time
					fileLineCache[0] = byteBuffer.get();
					fileLineCache[1] = byteBuffer.get();
					byteBuffer.position(byteBuffer.position() + 1);//移动一位
					fileLineCache[2] = byteBuffer.get();
					fileLineCache[3] = byteBuffer.get();
					endMonDay = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, 4);
					activityVo.setEndTime(endMonDay * 1000000 + 95959);
					byteBuffer.position(byteBuffer.position() + 20);//brandname最少10字符 20=10+10

					break;
				case 3://brandId
					getSegment(byteBuffer, fileLineCache);//空调用，忽略brand_name

					length = getSegment(byteBuffer, fileLineCache);
					long brandId = InvincibleConvertUtil.byteArrayToInt(fileLineCache, 0, length);
					long brandPlatform = (brandId << 2) + platform;
					long brandAct = (actName - 20170000000000000L) * 10000 + endMonDay;
					HashLongSet brandActSet = brandMap.get(brandPlatform);
					if(brandActSet != null){
						brandActSet.add(brandAct);
					}else{
						brandActSet = HashLongSets.newUpdatableSet(16);
						brandMap.put(brandPlatform, brandActSet);
						brandActSet.add(brandAct);
					}

					break;
			}

			if(segmentType == 3){//一行结束，重置
				segmentType = 0;
				activityVo = null;
				actPlatform = 0;

				row++;//// TODO: 2018/3/1
			}else{
				segmentType++;
			}
		}

		System.out.println("行数：" + row);
 	}

	private int getSegment(MappedByteBuffer inputBuffer, byte[] fileLineCache){
		int i = 0;
		try{
			for(; i < fileLineCache.length; i++){
				byte b = inputBuffer.get();
				if(b == Constants.SEGMENT_SEPARATE_SYMBOL || b == Constants.NEW_LINE_CHAR_ASCII){
					break;
				}
				fileLineCache[i] = b;
			}
		}catch (BufferUnderflowException e){
			//已经是结尾
		}
		return i;
	}

	public static void main(String[] args) throws InterruptedException {

//		int i = 1;
//		while (true){
//			Thread.sleep(5000L);
//			i++;
//			if(i > 2){
//				break;
//			}
//		}

		String fileName = "1-1.txt";
		if(args.length > 0){
			fileName = args[0];
		}

		long t1 = System.currentTimeMillis();
		long start = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		HashLongObjMap<ActivityVo> activityMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);
		HashLongObjMap<HashLongSet> brandMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);

		ActFileReader actFileReader = new ActFileReader(Constants.ACT_DATA_FILE, activityMap, brandMap);
		actFileReader.read();

		System.gc();
		Thread.sleep(2000L);

		long t2 = System.currentTimeMillis();
		long mid = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		System.out.println("time:" + (t2-t1) +  ",mem:" + (mid - start));

		LikeGoodsFileReader2 likeGoodsFileReader = new LikeGoodsFileReader2(Constants.LIKE_GODDS_DATA_FILE_PATH + fileName, activityMap, brandMap);
		try {
			likeGoodsFileReader.call();
		} catch (Exception e) {
			e.printStackTrace();
		}

		System.gc();
		Thread.sleep(2000L);

		long t3 = System.currentTimeMillis();
		long end = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();

		System.out.println("time:" + (t3-t2) + ",mem:" + (end - mid));
		System.out.println("actmap:" + activityMap.size() + ",brandmap:" + brandMap.size());

		//print
		TopKComputer.collectInfoAndPrint(activityMap);



		Thread.sleep(10000L);
	}



}
