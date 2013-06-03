package org.data.xinhuajie.mobilepay.mapreduce;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DefaultStringifier;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * 閫氫俊璐︽埛data鏃ュ織娓呮礂
 * 灏嗗惈鏈塓UERYINFO淇℃伅鐨勮鎸夌渷浠ｇ爜娓呮礂鍒颁笉鍚屾枃浠朵腑,鐢ㄤ簬涓嬩竴姝ョ户缁竻鐞嗗瓧娈靛�
 * @author mengxin
 * 
 */
public class MobilePayLogETL extends Configured implements Tool {
	private static Log logger = LogFactory.getLog(MobilePayLogETL.class);

	public static class MobilePayLogETLMap extends
			Mapper<Object, Text, Text, NullWritable> {
		private MultipleOutputs<Text, NullWritable> mos;

		private Map<String, String> provincemap = new HashMap<String, String>();
		
		private Text time;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			mos = new MultipleOutputs<Text, NullWritable>(context);
			time = DefaultStringifier.load(context.getConfiguration(), "time", Text.class);
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
							provincemap.put(value[0], value[0]);

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
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			mos.close();
			super.cleanup(context);
		}

		public void map(Object key, Text value,
				Mapper<Object, Text, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			String line = new String(value.getBytes(), 0, value.getLength(),"GBK");
			if (line.contains("QUERYINFO")) {
				String[] data = line.split(",");
				String fn = provincemap.get(data[3]);
				if (fn == null || fn.equals("")) {
					fn = "unknown";
				}
				Text outkey = new Text();
				outkey.set(line);
				mos.write(fn+time, outkey, NullWritable.get());
			}
		}
	}

	public static class MobilePayLogETLReduce extends
			Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Reducer<Text, NullWritable, Text, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}
	
	public int run(String[] args) throws Exception {
		if (args.length < 4) {
			System.err.println("Usage: mobilepaydatalogETL <xelist> <date> <in> ... <out>");
			return -1;
		}
		long startTime = System.currentTimeMillis();
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();// 鍙傛暟瑙ｆ瀽
		// DistributedCache淇濆瓨灏忛鐪佷唤鍒楄〃鏂囦欢鍐呭
		DistributedCache.createSymlink(conf);
		try {
			DistributedCache.addCacheFile(new URI(otherArgs[0]), conf);
		} catch (URISyntaxException e) {
			System.err.println(e);
			return -2;
		}
		
		DefaultStringifier.store(conf, new Text(otherArgs[1]) ,"time");

		Job job = new Job(conf, "mobilepaydatalogETL");
		job.setJarByClass(MobilePayLogETL.class);
		job.setMapperClass(MobilePayLogETLMap.class);
		job.setReducerClass(MobilePayLogETLReduce.class);
		for(int i=2;i<otherArgs.length-1;i++){
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length-1]));
		
		String uri = otherArgs[0];
		FileSystem fs = FileSystem.get(URI.create(otherArgs[0]),conf);
		InputStream in = null;
		in = fs.open(new Path(uri));
		BufferedReader reader = new BufferedReader(new InputStreamReader(in));
		String s = null;
		while ((s = reader.readLine()) != null) {
			String[] value = s.split("\t");
			System.out.println(value[0]);
			MultipleOutputs.addNamedOutput(job, value[0]+otherArgs[1],
					TextOutputFormat.class, Text.class, Text.class);
		}
		MultipleOutputs.addNamedOutput(job, "unknown"+otherArgs[1], TextOutputFormat.class,Text.class, Text.class);
		boolean commit = job.waitForCompletion(true);
		logger.info("mobilepaydatalog浠诲姟鎵ц瀹屾垚,鑰楁椂:"
				+ (System.currentTimeMillis() - startTime) + "姣");
		return commit ? 0 : 1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int ret = ToolRunner.run(new MobilePayLogETL(), args);
		System.exit(ret);
	}

}
