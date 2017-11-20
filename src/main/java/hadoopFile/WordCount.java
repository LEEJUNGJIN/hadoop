package hadoopFile;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


public class WordCount {

	public static class TokenCounterMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private Text word = new Text();
		private final IntWritable one = new IntWritable(1);
		//Map 오버라이드
		@Override
		protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			
				String line = value.toString();
				StringTokenizer itr = new StringTokenizer(line);
				while(itr.hasMoreTokens()) {
					word.set(itr.nextToken());
					context.write(word, one);
				}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		IntWritable result = new IntWritable();
		//reduce 함수 오버라이드
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
			
			int count = 0;
			for(IntWritable val:values) 
			count += val.get();
				
			result.set(count);
			context.write(key, result);
		}
		
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Word Count");

		job.setJarByClass(WordCount.class);
		job.setMapperClass(TokenCounterMapper.class);
		job.setReducerClass(IntSumReducer.class);
		/*job.setMapperClass(cls);
		job.setReducerClass(cls);
		job.setReducerClass(cls);*/
		
		//출력 데이터 형식 지정
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class); 
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileInputFormat.setInputPaths(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
		
	}

}
