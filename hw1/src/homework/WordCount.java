package homework;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
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

/**
 * WordCount Implementation on Hadoop MapReduce
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }

    /**
     * A function to run our MapReduce program
     */
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <input path> <output path>");
            return -1;
        }

        // create a MapReduce job
        Job job = Job.getInstance(getConf(), "WordCount");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
        job.setMapperClass(WordCountMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // combiner
        job.setCombinerClass(WordCountReducer.class); // we use our reducer implementation as combiner!

        // reducer
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // run
        return job.waitForCompletion(true) ? 0 : -1;
    }

    /**
     * Mapper Implementation for WordCount Job
     *
     * Input:  key   - a line number (type: LongWritable, not used)
     *         value - each line in the input file (type: Text)
     * Output: key   - a word (type: Text)
     *         value - 1 (type: IntWritable)
     */
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private Text keyOut = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // split line by a space character
            String line = value.toString();
            String[] words = line.split(" ");

            for (String word : words) {
                // write (word, 1) pair for each word in the line
                keyOut.set(word);
                context.write(keyOut, one);
            }
        }
    }

    /**
     * Reducer Implementation for WordCount Job
     *
     * Input:  key    - a word (type: Text)
     *         values - a list consists of partial word counts from each mapper (type: IntWritable)
     * Output: key    - a word (type: Text)
     *         value  - total number of  occurrences (type: IntWritable)
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable valueOut = new IntWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // sum the all received counts
            int sum = 0;
            for (IntWritable count : values) {
                sum += count.get();
            }
            valueOut.set(sum);

            // write a (word, count) pair to output file. The word and count are separated by a tab character.
            context.write(key, valueOut);
        }
    }
}

