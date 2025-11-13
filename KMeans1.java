import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeans1 {

    // Mapper: Assign each student to nearest centroid
    public static class KMeansMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        // Initial centroids: {math, science}
        private double[][] centroids = {{30, 30}, {80, 80}}; // Low performers, High performers

        @Override
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");
            if (parts.length < 3) return; // skip invalid lines

            String studentId = parts[0];
            double math = Double.parseDouble(parts[1]);
            double science = Double.parseDouble(parts[2]);

            double minDist = Double.MAX_VALUE;
            int clusterId = -1;

            // Find nearest centroid
            for (int i = 0; i < centroids.length; i++) {
                double dx = math - centroids[i][0];
                double dy = science - centroids[i][1];
                double dist = dx * dx + dy * dy;
                if (dist < minDist) {
                    minDist = dist;
                    clusterId = i;
                }
            }

            context.write(new IntWritable(clusterId), new Text(studentId + " " + math + " " + science));
        }
    }

    // Reducer: Output assigned cluster for each student
    public static class KMeansReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    // Driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeans Clustering");
        job.setJarByClass(KMeans1.class);

        job.setMapperClass(KMeansMapper.class);
        job.setReducerClass(KMeansReducer.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
