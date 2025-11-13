import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CharCount {

    public static class CharMapper
            extends Mapper<LongWritable, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text character = new Text();

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            char[] chars = value.toString().toCharArray();

            for (char c : chars) {
                if (!Character.isWhitespace(c)) {
                    character.set(String.valueOf(c));
                    context.write(character, one);
                }
            }
        }
    }

    public static class CharReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable v : values) {
                sum += v.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "character count");

        job.setJarByClass(CharCount.class);
        job.setMapperClass(CharMapper.class);
        job.setReducerClass(CharReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}






//javac -classpath $(hadoop classpath) -d classes CharCount.java
//jar -cvf charcount.jar -C classes/ .
//hadoop jar charcount.jar CharCount /user/ponny/input /user/ponny/ccoutput
///hadoop fs -cat /user/ponny/ccoutput/part-00000
