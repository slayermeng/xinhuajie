package org.data.xinhuajie.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ToolRunner;

public class DistributedGrep implements JobFactory {
	private static final Log logger = LogFactory.getLog(DistributedGrep.class);

	public static class RegexMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Pattern pattern;
		private int group;
		
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			Text reggroup = DefaultStringifier.load(conf, "group", Text.class);
			pattern = Pattern.compile(DefaultStringifier.load(conf, "regex", Text.class).toString());
			if(reggroup!=null){
				group = Integer.parseInt(reggroup.toString());
			}else{
				group = 0;
			}
			
		}

		public void map(LongWritable offset, Text value, Context context)
				throws IOException {
			String text = value.toString();
			Matcher matcher = pattern.matcher(text);
			while (matcher.find()) {
				try {
					context.write(new Text("match:" + matcher.group(group)),
							new Text(text));
				} catch (InterruptedException e) {
					logger.error(e);
				}
			}
		}

	}

	public static class RegexReduce extends
			Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			Iterator it = values.iterator();
			while (it.hasNext()) {
				context.write(key, new Text(it.next().toString()));
			}
			
		}
	}

	public Job buildSubmitJob(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Grep <inDir> <outDir> <regex> [<group>]");
			ToolRunner.printGenericCommandUsage(System.out);
			return null;
		}
		Configuration conf = new Configuration();
		DefaultStringifier.store(conf, new Text(args[0]) ,"regex");
		if (args.length == 4){
			DefaultStringifier.store(conf, new Text(args[2]) ,"group");
		}else{
			DefaultStringifier.store(conf, new Text("0") ,"group");
		}
		Job grepJob = new Job(conf, "grep-search");
		grepJob.setJarByClass(DistributedGrep.class);
		grepJob.setMapperClass(RegexMapper.class);
		grepJob.setReducerClass(RegexReduce.class);
		FileInputFormat.setInputPaths(grepJob, args[1]);
		if(args.length==4){
			FileOutputFormat.setOutputPath(grepJob, new Path(args[3]));
		}else{
			FileOutputFormat.setOutputPath(grepJob, new Path(args[2]));
		}
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(Text.class);
		grepJob.setNumReduceTasks(1);
		grepJob.submit();
		return grepJob;
	}
}
