package org.data.xinhuajie.mapreduce.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;

public class MapReduceClient{
	private MapReduceProtocol proxy;
	
	public MapReduceClient(String address,int port){
		InetSocketAddress addr = new InetSocketAddress(address,port);
		try{
			proxy = (MapReduceProtocol) RPC.waitForProxy(MapReduceProtocol.class, 821019, addr , new Configuration());
		}catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void close(){
		RPC.stopProxy(proxy);
	}
	
	public Text createJob(Text jobClass,String[] jobArgs) throws Exception{
		Text jobid = proxy.createJob(jobClass,jobArgs);
		return jobid;
	}
	
	public IntWritable queryJobState(Text jobId) throws IOException{
		return proxy.queryJobState(jobId);
	}
	
	public Text viewJobResult(Text path) throws IOException{
		return proxy.viewJobResult(path);
	}
	
	public boolean rmr(String dir) throws IOException{
		return proxy.rmr(dir);
	}
	
	public Text list(Text path) throws IOException {
		return proxy.list(path);
	}
}
