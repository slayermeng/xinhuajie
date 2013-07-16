package org.data.xinhuajie.mapreduce;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

/**
 * 定义创建Job的各种方式
 * @author yaojianhua
 * @since 2013-05-08
 */
public interface JobFactory
{
	/**
	 * 创建可以提交运行的Job
	 * @param args Job具体业务参数
	 * @return job 可提交运行的Job
	 */
	public Job buildSubmitJob(String[] args) throws Exception;
}
