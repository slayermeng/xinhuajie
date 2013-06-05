package org.data.xinhuajie.mobilepay.mapreduce;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DistributedGrep extends Configured implements Tool {
	private static final Log logger = LogFactory.getLog(DistributedGrep.class);
	
	public static class RegexMapper<K> extends MapReduceBase implements
			Mapper<K, Text, Text, Text> {

		private Pattern pattern;
		private int group;

		public void configure(JobConf job) {
			pattern = Pattern.compile(job.get("mapred.mapper.regex"));
			group = job.getInt("mapred.mapper.regex.group", 0);
		}

		public void map(K key, Text value, OutputCollector<Text, Text> output,
				Reporter reporter) throws IOException {
			String text = value.toString();
			Matcher matcher = pattern.matcher(text);
			while (matcher.find()) {
				output.collect(new Text("match:"+matcher.group(group)), new Text(text));
			}
		}

	}

	private DistributedGrep() {
	} // singleton

	public int run(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
			ToolRunner.printGenericCommandUsage(System.out);
			return -1;
		}

		JobConf grepJob = new JobConf(getConf(), DistributedGrep.class);
		grepJob.setJobName("grep-search");
		FileInputFormat.setInputPaths(grepJob, args[0]);
		FileOutputFormat.setOutputPath(grepJob, new Path(args[1]));
		grepJob.setMapperClass(RegexMapper.class);
		grepJob.setReducerClass(IdentityReducer.class);
		grepJob.set("mapred.mapper.regex", args[2]);
		if (args.length == 4)
			grepJob.set("mapred.mapper.regex.group", args[3]);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(Text.class);
		long startTime = System.currentTimeMillis();
		JobClient.runJob(grepJob);
		logger.info("DistributedGrep任务执行完成,耗时:"
				+ (System.currentTimeMillis() - startTime) + "毫秒");
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistributedGrep(),
				args);
		System.exit(res);
	}

}
