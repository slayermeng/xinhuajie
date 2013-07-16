package org.data.xinhuajie.mapreduce;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.io.Text;
import org.data.xinhuajie.mapreduce.rpc.MapReduceClient;

public class HDFSClient {
	private MapReduceClient client = null;
	
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
	
	public void list(String path) throws IOException{
		System.out.println(client.list(new Text(path)).toString());
	}
	
	public static void main(String[] args) throws Exception{
		HDFSClient c = new HDFSClient();
		c.list(args[0]);
	}
}
