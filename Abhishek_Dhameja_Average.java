import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Abhishek_Dhameja_Average {
	public static class MenuAverageMapper extends Mapper<Object, Text, Text, Text> {

		private Text event = new Text();
		private Text count = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			String[] record = value.toString().split(",");
			if (record[0].equals("id")) return;
			String eventname=record[3];
			eventname=eventname.replaceAll("\'", "");
			eventname=eventname.replaceAll("-", "");
			eventname=eventname.replaceAll("\\p{Punct}", " ");
			eventname=eventname.replaceAll("[^a-zA-Z0-9_ ]+","");
			eventname=eventname.trim();
			if(eventname.equals("")) return;
			eventname=eventname.toLowerCase();
			count.set(record[record.length-2]);
			event.set(eventname);
			context.write(event, count);
			
		}
	}

	public static class MenuAverageReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			double sum = 0; 
			int count=0;
			for (Text val : values) {
				sum += Double.parseDouble(val.toString());
				count+=1;
			}
				StringBuilder sb=new StringBuilder();
				double avg= sum/count;
				sb.append(count + "\t" + String.format("%.3f", avg));
				result.set(sb.toString());
				context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length < 2) {
			System.err.println("Usage: Abhishek_Dhameja_Average <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(conf, "menu average");
		job.setJarByClass(Abhishek_Dhameja_Average.class);
		job.setMapperClass(MenuAverageMapper.class);
		job.setReducerClass(MenuAverageReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; ++i) {
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		}
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
