package org.data.xinhuajie.mapreduce.rpc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.JobStatus;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapreduce.Job;
import org.data.xinhuajie.mapreduce.JobFactory;
import org.apache.hadoop.fs.Path;

public class MapReduceServer implements MapReduceProtocol {
	private Server server;

	public MapReduceServer(String address, int port) {
		try {
			server = RPC.getServer(this, address, port, new Configuration());
			server.start();
			server.join();
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	@Override
	public long getProtocolVersion(String protocol, long clientVersion)
			throws IOException {
		return 821019;
	}

	@Override
	public Text createJob(Text jobClass, String[] jobArgs) throws Exception {
		JobFactory jobFactory = null;
		try {
			jobFactory = (JobFactory) Class.forName(jobClass.toString())
					.newInstance();
		} catch (Exception e) {
			e.printStackTrace();
		}
		Text jobid = new Text();
		if (jobFactory != null) {
			Job runJob = jobFactory.buildSubmitJob(jobArgs);
			jobid.set(runJob.getJobID().toString());
		}
		return jobid;
	}

	@Override
	public IntWritable queryJobState(Text jobId) throws IOException {
		JobClient jobClient = new JobClient(new JobConf());
		RunningJob queryJob = jobClient.getJob(JobID.forName(jobId.toString()));
		int jobState = queryJob.getJobState();
		return new IntWritable(jobState);
	}

	@Override
	public Text viewJobResult(Text path) throws IOException {
		Text result = new Text();
		Configuration conf = new Configuration();
		String uri = conf.get("fs.default.name").toString() + "/" + path;
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream in = null;
		OutputStream out = new ByteArrayOutputStream();
		try {
			FileStatus[] status = fs.globStatus(new Path(uri + "/part*"));
			for (FileStatus f : status) {
				in = fs.open(f.getPath());
				IOUtils.copyBytes(in, out, conf);
			}
		} finally {
			IOUtils.closeStream(in);
		}

		result.set(out.toString());
		return result;
	}

	public boolean rmr(String dir) throws IOException {
		Configuration conf = new Configuration();
		String uri = conf.get("fs.default.name").toString() + "/" + dir;
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path srcPattern = new Path(dir);
		return fs.delete(srcPattern, true);
	}

	public Text list(Text path) throws IOException {
		Configuration conf = new Configuration();
		String uri = conf.get("fs.default.name").toString() + path.toString();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		FileStatus fileStats[] = fs.listStatus(new Path(path.toString()));
		SimpleDateFormat dateForm = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		int maxReplication = 3, maxLen = 10, maxOwner = 0,maxGroup = 0;
		StringBuffer sb = new StringBuffer();
		for (FileStatus stat : fileStats) {
			Path cur = stat.getPath();
			int replication = String.valueOf(stat.getReplication()).length();
	        int len = String.valueOf(stat.getLen()).length();
	        int owner = String.valueOf(stat.getOwner()).length();
	        int group = String.valueOf(stat.getGroup()).length();
	        
	        if (replication > maxReplication) maxReplication = replication;
	        if (len > maxLen) maxLen = len;
	        if (owner > maxOwner)  maxOwner = owner;
	        if (group > maxGroup)  maxGroup = group;
			String mdate = dateForm
					.format(new Date(stat.getModificationTime()));

			sb.append((stat.isDir() ? "d" : "-") + stat.getPermission()
					+ " ");
			sb.append(mdate + " ");
			sb.append(cur.toUri().getPath());
			sb.append("\n");
		}
		return new Text(sb.toString());
	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.out.println("usage:<ip> <port>");
			return;
		}
		new MapReduceServer(args[0], Integer.parseInt(args[1]));
	}

}
