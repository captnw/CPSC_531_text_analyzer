import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.*;

public class WordCounter extends Configured implements Tool {
    public static class WordTokenMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final boolean allLowercase = true;

        private final IntWritable one = new IntWritable(1); // stores the one value

        private final Set<String> ignorePatterns = new HashSet<String>();

        @Override
        public void setup(Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            // hardcoded for now
            ignorePatterns.add("\\.");
            ignorePatterns.add("\\,");
            ignorePatterns.add("!");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // Preprocess the sentence
            String line;
            if (allLowercase){
                line = value.toString().toLowerCase();
            } else {
                line = value.toString();
            }

            for (String pattern : ignorePatterns) {
                String oldline = line;
                line = line.replaceAll(pattern, "");
            }

            // Tokenize the sentence
            StringTokenizer itr = new StringTokenizer(line);

            // Iterate through all tokens and create a new Text, IntWritable tuple2
            while (itr.hasMoreTokens()) {
                Text current_word = new Text(itr.nextToken());
                // currently, there is a count of one for this word
                // this will tie in the reduce task
                //System.out.println(current_word);
                context.write(current_word, one);
            }
        }
    }

    public static class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // Since this is all one "partition" with the same word, just sum up all of the values
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: WordCounter <input path> <output path>");
            return -1;
        }

        // Create Configuration and MR Job objects
        Job job = Job.getInstance(getConf(), "Count frequency of each word in text");

        job.setJarByClass(WordCounter.class);

        job.setMapperClass(WordTokenMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        Long startTime = System.nanoTime();
        Boolean jobStatus = job.waitForCompletion(true);
        Long endTime = System.nanoTime();
        // Calculate time diff and convert nanoseconds to seconds
        Double elapsedTime = (endTime - startTime)/1e9d;

        System.out.println("WordCounter job took " + elapsedTime + " seconds");

        return jobStatus ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new WordCounter() ,args);
        System.exit(exitCode);
    }
}
