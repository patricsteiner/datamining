package homework;

import homework.types.TermDocumentPair;
import homework.types.TypedRecord;

import java.io.IOException;
import java.util.PriorityQueue;
import java.util.Queue;

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
        Job job = Job.getInstance(getConf(), "TFIDFJoin (2017-81517");

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

    /**
     * TFMapper Implementation for TFIDFJoin Job
     *
     * Input:  key   - unused (type: LongWritable)
     *         value - (word, document, tfScore) pair (type: Text)
     * Output: key   - a word (type: Text)
     *         value - a type value pair (type: TypedRecord)
     */
    public static class TFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
    	private Text keyOut = new Text();
    	private TypedRecord valueOut = new TypedRecord();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// input is received as (word, document, tfScore) pair, separated by tab
        	String[] input = value.toString().split("\t");
        	String word = input[0];
        	int document = new Integer(input[1]);
        	double score = new Double(input[2]);
        	keyOut.set(word);
        	valueOut.setTFScore(document, score);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * IDFMapper Implementation for TFIDFJoin Job
     *
     * Input:  key   - unused (type: LongWritable)
     *         value - (word, idfScore) pair (type: Text)
     * Output: key   - a word (type: Text)
     *         value - a type value pair (type: TypedRecord)
     */
    public static class IDFMapper extends Mapper<LongWritable, Text, Text, TypedRecord> {
    	private Text keyOut = new Text();
    	private TypedRecord valueOut = new TypedRecord();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	// input is received as (word, idfScore) pair, separated by tab
        	String[] input = value.toString().split("\t");
        	String word = input[0];
        	double score = new Double(input[1]);
        	keyOut.set(word);
        	valueOut.setIDFScore(score);
            context.write(keyOut, valueOut);
        }
    }

    /**
     * Reducer Implementation for TFIDFJoin Job
     *
     * Input:  key    - a word (type: Text)
     *         values - a list of TypedRecords (type: TypedRecord)
     * Output: key    - a word document pair (type: TermDocumentPair)
     *         value  - IDFTF score of this pair (type: DoubleWritable)
     */
    public static class TFIDFReducer extends Reducer<Text, TypedRecord, TermDocumentPair, DoubleWritable> {
    	TermDocumentPair keyOut = new TermDocumentPair();
    	DoubleWritable valueOut = new DoubleWritable();
    	
        @Override
        protected void reduce(Text key, Iterable<TypedRecord> values, Context context) throws IOException, InterruptedException {
        	double idfScore = -1; // will be changed as soon as IDF record is processed
        	Queue<TypedRecord> tfScores = new PriorityQueue<>(); // use a minheap for ordering by tfScore
        	String word = key.toString();
        	for (TypedRecord record : values) {
                switch (record.getType()) {
                case IDF: // happens only once per word
                	idfScore = record.getScore();
                	break;
                case TF: // can happen several times per word
                	tfScores.add(new TypedRecord(record));
                	break;
                }
            }
        	
        	for (int i = 0; i < 10 && !tfScores.isEmpty(); i++) {
        		TypedRecord tr = tfScores.poll();
        		keyOut.set(word, tr.getDocumentId());
        		valueOut.set(tr.getScore() * idfScore);
        		context.write(keyOut, valueOut);
        	}
        }
    }
}

