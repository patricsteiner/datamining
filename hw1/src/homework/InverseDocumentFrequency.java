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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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

    /**
     * Mapper Implementation for InverseDocumentFrequency Job
     *
     * Input:  key   - a document number (type: LongWritable)
     *         value - each word in the document (type: Text)
     * Output: key   - a word (type: Text)
     *         value - 1 (type: IntWritable, not used)
     */
    public static class IDFMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    	private Text keyOut = new Text();
        private IntWritable one = new IntWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// split line by a space character
            String line = value.toString();
            line = line.substring(line.indexOf('\t') + 1); // only use the words, ignore the document number
            String[] words = line.split(" ");

            // use a set to save all words that have already been written
            Set<String> alreadyWritten = new HashSet<>();
            
            for (String word : words) {
                // write (word, 1) once for each word in the line
            	if (!alreadyWritten.contains(word)) {
	            	alreadyWritten.add(word);
	                keyOut.set(word);
	                context.write(keyOut, one);
            	}
            }
        }
    }

    /**
     * Reducer Implementation for WordCount Job
     *
     * Input:  key    - a word (type: Text)
     *         values - a list of 1 (for each document the word occurs) (type: IntWritable)
     * Output: key    - a word (type: Text)
     *         value  - IDF score of this word (type: DoubleWritable)
     */
    public static class IDFReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        // you may assume that this variable contains the total number of documents.
        private int totalDocuments;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            totalDocuments = context.getConfiguration().getInt("totalDocuments", 1);
        }

        private DoubleWritable valueOut = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // sum the all received counts (using a loop since Iterable has no length property)
            int sum = 0;
            for (IntWritable count : values) {
                sum++;
            }
            
            double idfScore = Math.log((double)totalDocuments / sum);
            valueOut.set(idfScore);

            // write a (word, idfScore) pair to output file.
            context.write(key, valueOut);
        }
    }
}

