package homework;

import homework.types.TermDocumentPair;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TermFrequency extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new TermFrequency(), args);
        System.exit(exitCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: <input path> <output path>");
            return -1;
        }

        // create a MapReduce job (put your student id below!)
        Job job = Job.getInstance(getConf(), "TermFrequency (2017-81517)");

        // input
        job.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));

        // mapper
        job.setMapperClass(TFMapper.class);
        job.setMapOutputKeyClass(TermDocumentPair.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        // reducer
        job.setNumReduceTasks(0); // we use only mapper for this MapReduce job!
        job.setOutputKeyClass(TermDocumentPair.class);
        job.setOutputValueClass(DoubleWritable.class);

        // output
        job.setOutputFormatClass(TextOutputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true) ? 0 : -1;
    }

    /**
     * Mapper Implementation for TermFrequency Job
     *
     * Input:  key   - a document number (type: LongWritable)
     *         value - each word in the document (type: Text)
     * Output: key   - a word document pair (type: TermDocumentPair)
     *         value - the TF score of this pair (type: DoubleWritable)
     */
    public static class TFMapper extends Mapper<LongWritable, Text, TermDocumentPair, DoubleWritable> {
    	private TermDocumentPair keyOut = new TermDocumentPair();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// split line by a space character
            String line = value.toString();
            // first part of input is document number, extract it
            int docNum = new Integer(line.substring(0, line.indexOf('\t')));
            line = line.substring(line.indexOf('\t') + 1); // only use the words
            String[] words = line.split(" ");
            
            Map<String, Integer> wordCounts = new HashMap<>();
            int maxWordCount = 0;
            
            for (String word : words) {
            	// only count the word if it has not been counted yet
            	if (!wordCounts.containsKey(word)) {
	            	int wordCount = 0;
	            	// count how many times each word occurs
	            	for (String w : words) { 
	            		if (word.equals(w)) wordCount++;
	            	}
	            	// save the maxWordCount to later calculate TF score
	            	if (wordCount > maxWordCount) maxWordCount = wordCount;
	            	// save the wordCount in map
	            	wordCounts.put(word, wordCount);
            	}
            }
            
            // write (TermDocumentPair, score) for each word in the line
            for (Entry<String, Integer> entry : wordCounts.entrySet()) {
                keyOut.set(entry.getKey(), docNum);
                DoubleWritable score = new DoubleWritable(.5 + .5 * (double)entry.getValue() / maxWordCount);
                context.write(keyOut, score);
            }
        }
    }
}

