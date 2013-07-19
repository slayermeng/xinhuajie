package org.data.xinhuajie.mobilepay.mapreduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MobilePayFieldTransfer extends Configured implements Tool {
	private static Log logger = LogFactory.getLog(MobilePayFieldTransfer.class);
	
	public static class MobilePayFieldTransferMap extends
			Mapper<Object, Text, Text, NullWritable> {

		private static final String FIELDSEPARATE = "=";

		private Map<String, String> provincemap = new HashMap<String, String>();
		
		private MultipleOutputs<Text, NullWritable> mos;
		
		Pattern p = Pattern.compile("\\}$");
		Matcher m = null;

		private int splitField(String field) {
			int pos = field.lastIndexOf(FIELDSEPARATE);
			return pos;
		}
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
			try {
				Path[] cacheFiles = DistributedCache.getLocalCacheFiles(context
						.getConfiguration());
				if (null != cacheFiles && cacheFiles.length > 0) {
					String line;
					BufferedReader br = new BufferedReader(new FileReader(
							cacheFiles[0].toString()));
					try {
						while ((line = br.readLine()) != null) {
							String[] value = line.split("\t");
							provincemap.put(value[0], value[1]);

						}
					} finally {
						br.close();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
			super.setup(context);
		}
		
		@Override
		public void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			StringBuffer sb = new StringBuffer();
			for (int i = 0; i < fields.length; i++) {
				String s = fields[i];
				if (s.indexOf(FIELDSEPARATE) == -1) {
					sb.append(s);
				} else {
					m = p.matcher(s);
					int pos = splitField(s);
					if (m.find() == true) {// 结尾字符是}
						if (pos != -1) {
							sb.append(s.substring(pos + 1, s.length() - 1));
						}
					} else {
						sb.append(s.substring(pos + 1, s.length()));
					}
				}
				if (i != fields.length - 1) {
					sb.append(",");
				}
			}
			String[] checklength = provincemap.get(fields[5]).split("\\|");
			if(Integer.parseInt(checklength[0])==fields.length){
				mos.write(fields[5]+checklength[0], new Text(sb.toString()), NullWritable.get());
				context.getCounter("datacompletion", "fullfields").increment(1);
				return;
			}
			if(checklength.length>1&&Integer.parseInt(checklength[1])==fields.length){
				mos.write(fields[5]+checklength[1], new Text(sb.toString()), NullWritable.get());
				context.getCounter("datacompletion", "fullfields").increment(1);
			}else{
				logger.info("不合格数据"+line);
				context.getCounter("datacompletion", "lackfields").increment(1);
			}
		}
		
		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}
	}

	public static class MobilePayFieldTransferReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: mobilepaytransfer <xelist> <in> ... <out>");
			return -1;
		}
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		// DistributedCache保存小额省份列表文件内容
		DistributedCache.createSymlink(conf);
		try {
			DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);
		} catch (URISyntaxException e) {
			System.err.println(e);
			return -2;
		}
		Job job = new Job(conf, "mobilepaytransfer");
		job.setJarByClass(MobilePayFieldTransfer.class);
		job.setMapperClass(MobilePayFieldTransferMap.class);
		job.setReducerClass(MobilePayFieldTransferReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for (int i = 1; i < otherArgs.length - 1; i++) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(
				otherArgs[otherArgs.length - 1]));
		
		String uri = otherArgs[0];
		FileSystem fs = FileSystem.get(URI.create(otherArgs[0]),conf);
		InputStream in = null;
		in = fs.open(new Path(uri));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String s = null;
		while ((s = reader.readLine()) != null) {
			String[] value = s.split("\t");
			String[] value1 = value[1].split("\\|");
			for(int i=0;i<value1.length;i++){
				MultipleOutputs.addNamedOutput(job, value[0]+value1[i],
						TextOutputFormat.class, Text.class, Text.class);
			}
		}
		
		boolean commit = job.waitForCompletion(true);
		logger.info("mobilepaytransfer任务执行完成,耗时:"
				+ (System.currentTimeMillis() - startTime) + "毫秒");
		return commit ? 0 : 1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MobilePayFieldTransfer(), args);
		System.exit(ret);
	}
}
