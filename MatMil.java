import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/*
Example:
A 0 0 1
A 0 1 2
A 1 0 3
A 1 1 4

B 0 0 5
B 0 1 6
B 1 0 7
B 1 1 8
*/


public class MatrixMultiplication {

    public static class MatrixMapper extends Mapper<LongWritable, Text, Text, Text> {

        int M = 2;  // number of rows in A
        int N = 2;  // number of columns in B

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");

            String matrix = parts[0];        // A or B
            int row = Integer.parseInt(parts[1]);
            int col = Integer.parseInt(parts[2]);
            double val = Double.parseDouble(parts[3]);

            if (matrix.equals("A")) {
                // A[i][k] contributes to all C[i][j]
                for (int j = 0; j < N; j++) {
                    context.write(new Text(row + "," + j),
                            new Text("A," + col + "," + val));
                }
            } else {
                // B[k][j] contributes to all C[i][j]
                for (int i = 0; i < M; i++) {
                    context.write(new Text(i + "," + col),
                            new Text("B," + row + "," + val));
                }
            }
        }
    }


    public static class MatrixReducer extends Reducer<Text, Text, Text, DoubleWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            HashMap<Integer, Double> A = new HashMap<>();
            HashMap<Integer, Double> B = new HashMap<>();

            // Collect values from mapper
            for (Text val : values) {
                String[] parts = val.toString().split(",");
                String matrix = parts[0];
                int index = Integer.parseInt(parts[1]);
                double value = Double.parseDouble(parts[2]);

                if (matrix.equals("A"))
                    A.put(index, value);
                else
                    B.put(index, value);
            }
	    //multiply

            double sum = 0;
            for (int k : A.keySet()) {
                if (B.containsKey(k)) {
                    sum += A.get(k) * B.get(k);
                }
            }

            context.write(key, new DoubleWritable(sum));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Matrix Multiplication");

        job.setJarByClass(MatrixMultiplication.class);
        job.setMapperClass(MatrixMapper.class);
        job.setReducerClass(MatrixReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));   
        FileOutputFormat.setOutputPath(job, new Path(args[1])); 

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



//javac -classpath $(hadoop classpath) -d classes MatrixMultiplication.java
//jar -cvf matrixmul.jar -C classes/ .

