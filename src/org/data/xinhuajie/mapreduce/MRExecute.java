package org.data.xinhuajie.mapreduce;

import org.data.xinhuajie.mapreduce.rpc.MapReduceClient;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobStatus;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MRExecute {
	private static Map mrexecutor = new HashMap();	
	
	private static Map mrusage = new HashMap();
	
	private MapReduceClient client = null;

	private boolean isRunning = true;

	private Text buildJob(String key,String[] args) throws Exception {
		return client.createJob(new Text(mrexecutor.get(key).toString()), args);
	}
	
	static{
		mrexecutor.put("grep", "org.data.xinhuajie.mapreduce.DistributedGrep");
		mrusage.put("grep", "grep <regex> <inDir> [<group>]");
	}
	
	{
		Properties prop = new Properties();
		try {
			prop.load(new FileReader("server.properties"));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		String address = prop.getProperty("server");
		int port = Integer.parseInt(prop.getProperty("port"));
		client = new MapReduceClient(address,port);
	}

	private int queryJobState(Text jobid) throws Exception,
			InterruptedException {
		IntWritable result = null;
		result = client.queryJobState(jobid);
		if (result.get() == JobStatus.SUCCEEDED) {
			return 1;
		} else if (result.get() == JobStatus.KILLED
				|| result.get() == JobStatus.FAILED) {
			return -1;
		}else{
			return 0;
		}
	}

	private static String buildOutputDir() throws UnknownHostException{
		return "/"+InetAddress.getLocalHost().getHostAddress().replace(".", "")+System.currentTimeMillis();
	}
	
	private void stop() {
		client.close();
	}

	private void printResult(String outputDir) throws IOException {
		System.out.println(client.viewJobResult(new Text(outputDir)));
	}

	private static void printUsage(String key){
		System.out.println(mrusage.get(key));
	}
	
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			printUsage(args[0]);
			return;
		}
	
		String[] execArgs = new String[args.length];
		for(int i=1;i<args.length;i++){
			execArgs[i-1] = args[i];
		}
		String outputdir = buildOutputDir();
		execArgs[execArgs.length-1] = outputdir;
		MRExecute mr = new MRExecute();
		try {
			// 执行grep任务
			Text jobid = mr.buildJob(args[0],execArgs);
			// 查询job状态
			int state = 0;
			while (mr.isRunning) {
				state = mr.queryJobState(jobid);
				if (state == 1) {
					mr.printResult(outputdir);
					mr.client.rmr(outputdir);
					mr.isRunning = false;
				} else if (state == -1) {
					System.out.println("job failed!");
					mr.isRunning = false;
				}
				Thread.sleep(10 * 1000);
			}
			// 关闭client
		} finally {
			mr.stop();
		}
	}
}
