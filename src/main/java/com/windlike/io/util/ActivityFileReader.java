package com.windlike.io.util;

import com.koloboke.collect.map.hash.HashLongObjMap;
import com.koloboke.collect.map.hash.HashLongObjMaps;
import com.windlike.io.Constants;
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
public class ActivityFileReader{
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

	private HashLongObjMap<ActivityVo> hashLongObjMaps;

	private int row = 0;//行数

	public ActivityFileReader(String orignFileFullName) {
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
		row ++;

//		try{
//			List<String> tmp = StringConvertUtil.split(oneLineData, ' ', 8);
//
//			//获取品牌名hash
//			long brandNameHash = BKDRHash(getBrandName(tmp));
//
//			//销售额
//			long sellNum = StringConvertUtil.stringToLong(tmp.get(tmp.size() - 2));//Long.parseLong(tmp[tmp.length - 2]);
//
//			//获取时间数值
//			short date = parseDateToUniqueNum(tmp.get(tmp.size() - 1));
//
//			//发布到队列
//			byte index = (byte) (brandNameHash & (processThreadNum - 1));
//
//			Test2Data test2Data = hashLongObjMaps[index].get(brandNameHash);
//
//			if(test2Data != null){
//				test2Data.addToSum(sellNum);
//				test2Data.addToDateSet(date);
//			}
////			LineEventProducer.onData(disruptors[index].getRingBuffer(), brandNameHash, sellNum, date);
//		}catch (Exception e){
//			e.printStackTrace();
//		}
	}


	public void readAndProcess(MappedByteBuffer inputBuffer){
		byte[] fileLineCache = new byte[200];
		int length = 0;
		while((length = getLine(inputBuffer, fileLineCache)) > 0){
			sendToConsumers(new String(fileLineCache, 0, length));
		}
 	}

	private int getLine(MappedByteBuffer inputBuffer, byte[] fileLineCache){
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

}
