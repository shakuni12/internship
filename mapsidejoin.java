import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class joinmapper extends Mapper<LongWritable, Text, LongWritable, Text> {
Text outvalue = new Text();
LongWritable outkey = new LongWritable();
HashMap<Integer,String> userdata = new HashMap<Integer,String>();
@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	Path[] files= DistributedCache.getLocalCacheFiles(context.getConfiguration());
	for(Path file : files)
	{
		if(file.getName().equals("cust_details"))
		{
			BufferedReader reader = new BufferedReader(new FileReader(file.toString()));
			String line = reader.readLine();
			while(line != null)
			{
				String cols[] = line.split(",");
				int userid = Integer.parseInt(cols[0]);
				String name = cols[1];
				userdata.put(userid,name);
				line = reader.readLine();
			}reader.close();
		}
	}
	}
@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, LongWritable, Text>.Context context)
				throws IOException, InterruptedException {
String cols [] = key.toString().split(",");
int userid = Integer.parseInt(cols [2]);
String name = userdata.get(userid);
String product = cols[4];
outkey.set(userid);
outvalue.set("name " +name+ "  product" +product);

context.write(outkey,outvalue);
		}
}
public class joinjob implements Tool {
	private Configuration conf;
	@Override
	public Configuration getConf() {
		
		return conf;
	}@Override
	public void setConf(Configuration conf) {
		
		this.conf = conf;
	}@Override
	public int run(String[] args) throws Exception {
		Job j = new Job(getConf());
		j.setJobName("shakuni");
		j.setJarByClass(this.getClass());
		j.setMapperClass(joinmapper.class);
		DistributedCache.addCacheFile(new URI("/home/palash/Desktop/cust_details"), conf);
		j.setNumReduceTasks(0);
		j.setMapOutputKeyClass(LongWritable.class);
		j.setMapOutputValueClass(Text.class);

		FileInputFormat.addInputPath(j,new Path(args[0]));
		FileOutputFormat.setOutputPath(j, new Path(args[1]));
		return j.waitForCompletion(true)?0:-1;
	}
	public static void main(String[] args) throws Exception {
		int status = ToolRunner.run(new Configuration(),new joinjob(),args);
		System.out.println(status);
		
	}
