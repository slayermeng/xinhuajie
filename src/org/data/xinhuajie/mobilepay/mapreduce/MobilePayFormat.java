package org.data.xinhuajie.mobilepay.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MobilePayFormat extends Configured implements Tool {
	private static Log logger = LogFactory.getLog(MobilePayFormat.class);
	
	public static class MobilePayFormatMap extends
			Mapper<Object, Text, Text, NullWritable> {
		
		private static final String FIELDSEPARATE = "=";
		
		Pattern p = Pattern.compile("\\}$");
		Matcher m = null;
		
		private int splitField(String field){
			int pos = field.lastIndexOf(FIELDSEPARATE);
			return pos;
		}
		
		public void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] fields = line.split(",");
			StringBuffer sb = new StringBuffer();
			for(int i=0;i<fields.length;i++){
				if(i==2){
					sb.append("");
					sb.append(",");
					sb.append(fields[20].split("=")[1]);
					sb.append(",");
					sb.append(fields[i]);
				}else{
					sb.append(fields[i]);
				}
				if(i!=fields.length-1){
					sb.append(",");
				}
			}
			context.write(new Text(sb.toString()), NullWritable.get());
		}
	}

	public static class MobilePayFormatReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public int run(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: mobilepayformat <in> ... <out>");
			return -1;
		}
		long startTime = System.currentTimeMillis();
		String[] otherArgs = new GenericOptionsParser(getConf(), args).getRemainingArgs();
		Job job = new Job(getConf(), "mobilepayformat");
		job.setJarByClass(MobilePayFormat.class);
		job.setMapperClass(MobilePayFormatMap.class);
		job.setReducerClass(MobilePayFormatReduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		for(int i=0;i<otherArgs.length-1;i++){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		boolean commit = job.waitForCompletion(true);
		logger.info("mobilepaytransfer任务执行完成,耗时:"
				+ (System.currentTimeMillis() - startTime) + "毫秒");
		return commit?0:1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MobilePayFormat(), args);
		System.exit(ret);
	}
}
