package com.windlike.io.reader;

import com.carrotsearch.sizeof.RamUsageEstimator;
import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.windlike.io.Constants;
import com.windlike.io.Platforms;
import com.windlike.io.util.InvincibleConvertUtil;
import com.windlike.io.util.SizeOfUtil;
import com.windlike.io.vo.ActivityVo;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;

/**
 * 文件读写预处理任务
 * @author windlike.xu
 *
 */
public class ActFileReader3 {
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

	public HashLongObjMap<ActivityVo> getHashLongObjMaps() {
		return hashLongObjMaps;
	}

	private HashLongObjMap<ActivityVo> hashLongObjMaps;

	private int row = 0;//行数

	public ActFileReader3(String orignFileFullName) {
		this.orignFileFullName = orignFileFullName;
		hashLongObjMaps = HashLongObjMaps.newUpdatableMap(Constants.DEFAULT_ACT_SIZE);

//		this.beginFilePointer = beginFilePointer;
//		this.endFilePointer = endFilePointer;
//		this.hashLongObjMaps = hashLongObjMaps;
//		processThreadNum = this.hashLongObjMaps.length;
	}

	public Void call() {
		//启动读写任务
		try {
			File file  = new File(orignFileFullName);
			FileInputStream orginFile = new FileInputStream(file);
			MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, 0, file.length());

//			int length = (int) (endFilePointer- beginFilePointer + 1);
//			MappedByteBuffer inputBuffer = orginFile.getChannel().map(FileChannel.MapMode.READ_ONLY, beginFilePointer, length);//需要注意length不能大于2G哦哦哦哦哦@！！
//			spiltFileLine(inputBuffer, beginFilePointer, length);
			readAndProcess(inputBuffer);

			System.out.println("此任务完成，解释行数：" + row);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return null;
	}


	private void sendToConsumers(String oneLineData){
		row++;

		List<String> pieces = InvincibleConvertUtil.split(oneLineData, ',', 6);
		int platform = Platforms.shortNameToIndex(pieces.get(0).charAt(1));
		String actName = pieces.get(1);
		long actPlatform = (InvincibleConvertUtil.stringToLong(actName) << 2) + platform;
		ActivityVo activityVo = hashLongObjMaps.get(actPlatform);
		if(activityVo == null){
			activityVo = new ActivityVo();
			hashLongObjMaps.put(actPlatform, activityVo);

			activityVo.setActPlatfrom(actPlatform);
			activityVo.setStartTime(InvincibleConvertUtil.stringToInt(actName.substring(4, 8)) * 1000000 + 100000);
			String endStr = pieces.get(3);
			activityVo.setEndTime((((byte)endStr.charAt(5) - 48) * 1000000000 + ((byte)endStr.charAt(6) - 48) * 100000000 + ((byte)endStr.charAt(8) - 48) * 10000000 + 1000000 * ((byte)endStr.charAt(9) - 48))
					+ 95959);
//			activityVo.addBrandId(InvincibleConvertUtil.stringToInt(pieces.get(5)));
		}else{
			int i = 0;
		}
//		activityVo.addBrandId(InvincibleConvertUtil.stringToInt(pieces.get(5)));
	}

	protected String getBrandName(List<String> oneLineSplits) {
		StringBuilder brandName = new StringBuilder(25);
		for (int i = 0; i < oneLineSplits.size() - 4; i++) {
			brandName.append(" " + oneLineSplits.get(i));
		}
		return brandName.substring(1);
	}


	public void readAndProcess(MappedByteBuffer byteBuffer){
		byte[] fileLineCache = new byte[200];
		int length = 0;
		while((length = getSegment(byteBuffer, fileLineCache)) > 0){
			sendToConsumers(new String(fileLineCache, 0, length));
		}
		System.out.println("行数：" + row);
 	}

	private int getSegment(MappedByteBuffer inputBuffer, byte[] fileLineCache){
		int i = 0;
		try{
			for(; i < fileLineCache.length; i++){
				byte b = inputBuffer.get();
				if(b == Constants.NEW_LINE_CHAR_ASCII){
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
//
//		long t1 = System.currentTimeMillis();
//		ActFileReader3 actFileReader = new ActFileReader3(Constants.ACT_DATA_FILE);
//		actFileReader.call();
//		HashLongObjMap<ActivityVo> map = actFileReader.getActivityMap();
//
//		long t2 = System.currentTimeMillis();
//
//		System.out.println("1:" + (t2-t1) + ",2:" + map.size());

		ActivityVo[] activityVos = new ActivityVo[10];
		activityVos[0] = new ActivityVo();
		activityVos[2] = new ActivityVo();
		System.out.println(RamUsageEstimator.sizeOf(activityVos));
	}
}
