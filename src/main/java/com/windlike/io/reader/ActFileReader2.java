package com.windlike.io.reader;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.koloboke.collect.set.hash.HashLongSet;
import com.koloboke.collect.set.hash.HashLongSets;
import com.windlike.io.Constants;
import com.windlike.io.Platforms;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.vo.ActivityVo;

import java.io.*;
import java.util.List;

/**
 * 文件读写预处理任务
 * @author windlike.xu
 *
 */
public class ActFileReader2 {
	/**
	 * 读取开始的点
	 */
	private long beginFilePointer;
	/**
	 * 读取结束的点
	 */
	private long endFilePointer;

	private int processThreadNum;

	private String orignFileFullName;

	private HashLongObjMap<ActivityVo> activityMap;

	private HashLongObjMap<HashLongSet> brandMap;//val为'MMddHHmmssSSS(actname)|MMdd(endtime)'的list

	private int row = 0;//行数

	public ActFileReader2(String orignFileFullName , HashLongObjMap<ActivityVo> activityMap, HashLongObjMap<HashLongSet> brandMap) {
		this.orignFileFullName = orignFileFullName;
		this.activityMap = activityMap;
		this.brandMap = brandMap;
	}

	public Void call() {
		//启动读写任务
		try (BufferedReader br = new BufferedReader(new FileReader(new File(orignFileFullName)))){
			String str = null;
			while((str = br.readLine()) != null){
				row++;

				List<String> pieces = InvincibleConvertUtil.split(str, ',', 6);
				int platform = Platforms.shortNameToIndex(pieces.get(0).charAt(1));
				long actName = InvincibleConvertUtil.stringToLong(pieces.get(1));
				long actPlatform = (actName << 2) + platform;
				ActivityVo activityVo = activityMap.get(actPlatform);
				String endStr = pieces.get(3);
				if(activityVo == null){
					activityVo = new ActivityVo();
					activityMap.put(actPlatform, activityVo);

					activityVo.setActPlatfrom(actPlatform);
					activityVo.setStartTime(InvincibleConvertUtil.stringToInt(pieces.get(1).substring(4, 8)) * 1000000 + 100000);
					activityVo.setEndTime((((byte)endStr.charAt(5) - 48) * 1000000000 + ((byte)endStr.charAt(6) - 48) * 100000000 + ((byte)endStr.charAt(8) - 48) * 10000000 + 1000000 * ((byte)endStr.charAt(9) - 48))
							+ 95959);


				}
				int endMonDay = ((byte)endStr.charAt(5) - 48) * 1000 +  ((byte)endStr.charAt(6) - 48) * 100 + ((byte)endStr.charAt(8)- 48) * 10 + ((byte)endStr.charAt(9) - 48);
				long brandId = InvincibleConvertUtil.stringToInt(pieces.get(5));
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

//				activityVo.addBrandId(InvincibleConvertUtil.stringToInt(pieces.get(5)));
			}
//			int length = (int) (endFilePointer- beginFilePointer + 1);
//			MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, beginFilePointer, length);//需要注意length不能大于2G哦哦哦哦哦@！！
//			spiltFileLine(inputBuffer, beginFilePointer, length);
//			readAndProcess(inputBuffer);

			//清理
			activityMap = null;
			brandMap = null;

			System.out.println("此任务完成，解释行数：" + row);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
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
//
//		long t1 = System.currentTimeMillis();
//
//		HashLongObjMap<ActivityVo> activityMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);
//		HashLongObjMap<HashLongSet> brandMap = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);
//		ActFileReader2 actFileReader = new ActFileReader2(Constants.ACT_DATA_FILE, activityMap, brandMap);
//		actFileReader.call();
//
//		long t2 = System.currentTimeMillis();
//		System.out.println("time:" + (t2-t1));
//		System.out.println("actmap:" + activityMap.size() + ",brandmap:" + brandMap.size());

		String endStr = "2017-05-26 09:59:59";
		int endMonDay = ((byte)endStr.charAt(5) - 48) * 1000 +  ((byte)endStr.charAt(6) - 48) * 100 + ((byte)endStr.charAt(8)- 48) * 10 + ((byte)endStr.charAt(9) - 48);
		System.out.println(endMonDay);

//		System.out.println(20170623193117511L << 2);
//		long act = (20170623193117511L << 2) + 1;
//		System.out.println(act);
//		int plat = (int) (act & 3);
//		long actName = (long) (act>>2);
//		System.out.println(plat+","+actName);
//
//		long l = 80682492772470044L + 1L;
//		System.out.println(l);
	}
}
