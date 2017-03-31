package homework;

import homework.types.TermDocumentPair;
import homework.types.TypedRecord;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.DelegatingInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class TFIDFJoin extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TFIDFJoin(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: <tf score path> <idf score path> <output path>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "TFIDFJoin (<PUT YOUR STUDENT ID HERE>");

        // input & mapper
        job.setInputFormatClass(DelegatingInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TFMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, IDFMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(TypedRecord.class);

        // reducer
        job.setReducerClass(TFIDFReducer.class);
        job.setOutputKeyClass(TermDocumentPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    public static class TFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
        // fill your code here!
    }

    public static class IDFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
        // fill your code here!
    }

    public static class TFIDFReducer extends Reducer<Text, TypedRecord, TermDocumentPair, DoubleWritable> {
        // fill your code here!
    }
}

