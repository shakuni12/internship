import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class wordcountmapper extends Mapper<LongWritable, Text, Text,LongWritable> {
@Override
protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, LongWritable>.Context context)
		throws IOException, InterruptedException {
	String line = value.toString();
	String[] words = line.split(" ");
	for(String word : words)
	{
		context.write(new Text(word), new LongWritable(1));
	}
}
}


public class wordcountreducer extends Reducer<Text, LongWritable, Text, LongWritable> {
@Override
protected void reduce(Text key, Iterable<LongWritable> values,
		Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException, InterruptedException {
long  sum=0;
for(LongWritable value : values)
{
	sum = sum + value.get();
	
}
context.write(key ,new LongWritable(sum));
	
}
}

public class wordcountjob implements Tool {
	private Configuration conf;
	@Override
	public Configuration getConf() {
		
		return conf;
	}
	@Override
	public void setConf(Configuration conf) {
		this.conf = conf;
		
	}
	@Override
	public int run(String[] args) throws Exception {
		Job j = new Job(getConf());
		j.setJobName("shakuni");
		j.setJarByClass(this.getClass());
		j.setMapperClass(wordcountmapper.class);
		j.setReducerClass(wordcountreducer.class);
		j.setMapOutputKeyClass(Text.class);
		j.setMapOutputValueClass(LongWritable.class);
		j.setOutputKeyClass(Text.class);
		j.setOutputValueClass(LongWritable.class);
		j.setInputFormatClass(TextInputFormat.class);
		j.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j,new Path(args[1]));
		
		return j.waitForCompletion(true) ? 0 : -1;
	}
  
  public static void main(String[] args)throws Exception {
int status = ToolRunner.run(new Configuration(), new wordcountjob(), args);
System.out.println("my status"+status);
	}
	}

