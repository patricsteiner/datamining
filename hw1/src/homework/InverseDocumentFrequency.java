package homework;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class InverseDocumentFrequency extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new InverseDocumentFrequency(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: <input path> <output path> <number of documents>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "InverseDocumentFrequency (<PUT YOUR STUDENT ID HERE>)");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
        job.setMapperClass(IDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // reducer
        job.setReducerClass(IDFReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // passing number of documents
        job.getConfiguration().setInt("totalDocuments", Integer.parseInt(args[2]));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        // fill your code here!
    }

    public static class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        // you may assume that this variable contains the total number of documents.
        private int totalDocuments;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocuments = context.getConfiguration().getInt("totalDocuments", 1);
        }

        // fill your code here!
    }
}

