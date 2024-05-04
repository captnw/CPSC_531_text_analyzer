import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.LoggerFactory;


public class SentenceScorer extends Configured implements Tool {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SentenceMapper.class);

    public static class DescendingIntWritableComparable extends IntWritable {
        // Class is used to assist with iterating through the output keys of the map class
        // (The sentence scores) in descending order, this will allow us to quickly find
        // the most valued sentences and ignore the rest if possible
        public static class DescendingComparator extends Comparator {
            public int compare(WritableComparable wc1, WritableComparable wc2) {
                // Add negative sign to make sure IntWritable keys are sorted in descending order
                return -super.compare(wc1, wc2);
            }

            public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
                // Another template we need to override for the comparator
                // Just leave everything else alone and add a negative sign to the front
                // for descending order
                return -super.compare(b1, s1, l1, b2, s2, l2);
            }
        }
    }

    public static class SentenceMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

        private final Map<String, Integer> wordCount = new HashMap<>();

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            Configuration conf = context.getConfiguration();
            URI[] cached_files = Job.getInstance(conf).getCacheFiles();

            // There should only be one cached file (which is used to share the wordCount file with the map job)
            if (cached_files.length != 1) {
                throw new IllegalArgumentException("Sentence scorer's 4th argument expects only one file");
            }

            FileSystem fs = FileSystem.get(conf);
            URI wordCountURI = cached_files[0];
            Path wordCountFilePath = new Path(wordCountURI.getPath());

            // Have a try-with-resources so resources get closed properly if exception is thrown
            try (InputStreamReader isr = new InputStreamReader(fs.open(wordCountFilePath));
                 BufferedReader br = new BufferedReader(isr)){
                // Read the word count file, compatible with local file system and HDFS
                System.out.println("parsing wordCountfile ...");
                String line;
                int numLines = 0;

                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    wordCount.put(parts[0], Integer.parseInt(parts[1]));
                    numLines++;
                }
                System.out.println("processed " + numLines + " lines in wordCountfile");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // We score each sentence, where each parsed word's occurrence in the whole text
            // would add up to the cumulative score
            int score = 0;
            String sentence = "";
            sentence = value.toString();
            String[] words = sentence.split("\\s+");
            for (String word : words) {
                // Lookup word count and accumulate score
                if (wordCount.containsKey(word)) {
                    score += wordCount.get(word);
                }
            }
            LOG.info(sentence);
            // Show sentence with score
            context.write( new IntWritable(score), new Text(sentence));
        }
    }

    public static class ScorerReducer extends Reducer<IntWritable, Text, Text, Text> {
        private static final Integer topNSentenceDefault = 10; // by default, only display the top 10 sentences
        private Integer topNSentence;
        private int sentenceCount = 0;

        @Override
        protected void setup(Reducer.Context context) {
            // Load in the number of sentences we want in our summary
            Configuration conf = context.getConfiguration();
            topNSentence = conf.getInt("sentencescorer.reduce.numSentencesInSummary", topNSentenceDefault);
        }

        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // While we want the sentences in our summary, keep them in
                // At this point, the keys should be sorted, so we are processing in ascending
                // order
                if (sentenceCount == topNSentence) {
                    break;
                }
                sentenceCount++;
                context.write(new Text(String.valueOf(key)), value);
            }
        }
    }

    public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: SentenceScorer <input path> <output path> <wordcount file> <num sentences to display>");
            return -1;
        }

        // Create Configuration and MR Job objects
        Job job = Job.getInstance(getConf(), "Score each sentence given a word count file");

        job.setJarByClass(SentenceScorer.class);

        job.setMapperClass(SentenceMapper.class);
        // We want to sort the map output by their keys
        job.setMapOutputKeyClass(IntWritable.class);
        // We want to sort the keys in descending order
        job.setSortComparatorClass(DescendingIntWritableComparable.DescendingComparator.class);

        job.setMapOutputValueClass(Text.class);
        job.setReducerClass(ScorerReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Here are our inputs, we expect 4 inputs
        Path inputDir = new Path(args[0]);
        Path outputDir = new Path(args[1]);
        URI wordCountFile = new Path(args[2]).toUri();
        int numSentencesInSummary = Integer.parseInt(args[3]);

        FileInputFormat.addInputPath(job, inputDir);
        FileOutputFormat.setOutputPath(job, outputDir);

        // Add a cache file and a configuration to use later
        job.addCacheFile(wordCountFile); // Used in scoring during map phase
        // Set configuration for usage in reduce task
        job.getConfiguration().setInt("sentencescorer.reduce.numSentencesInSummary",numSentencesInSummary);

        // Measure how long the job takes
        Long startTime = System.nanoTime();
        Boolean jobStatus = job.waitForCompletion(true);
        Long endTime = System.nanoTime();
        // Calculate time diff and convert nanoseconds to seconds
        Double elapsedTime = (endTime - startTime)/1e9d;

        System.out.println("This job took " + elapsedTime + " seconds");

        return jobStatus ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int exitCode = ToolRunner.run(conf, new SentenceScorer() ,args);
        System.exit(exitCode);
    }
}
