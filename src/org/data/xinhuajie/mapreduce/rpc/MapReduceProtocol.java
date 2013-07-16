package org.data.xinhuajie.mapreduce.rpc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;
import org.apache.hadoop.io.BooleanWritable;

public interface MapReduceProtocol extends VersionedProtocol{
	/**
	 * 创建job
	 * @return
	 */
	public Text createJob(Text jobClass,String[] jobArgs) throws Exception;
	
	/**
	 * 任务是否已经完成
	 * @param jobId
	 * @return
	 */
	public IntWritable queryJobState(Text jobId) throws IOException;
	
	/**
	 * 查看任务结果
	 * @param jobId
	 * @return
	 */
	public Text viewJobResult(Text path) throws IOException;
	
	/**
	 * 递归删除指定目录
	 * @param dir
	 * @return
	 */
	public boolean rmr(String dir) throws IOException;
	
	/**
	 * 显示目录或文件
	 * @param path
	 * @return
	 * @throws IOException
	 */
	public Text list(Text path) throws IOException;
	
}
