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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.net.URI;

public class SentenceScorer {
    public static class SentenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //System.out.println(key + " test " + value);

            context.write(new Text("" +key), value);
        }
    }

    public static class ScorerReducer extends Reducer<Text, Text, Text, Text> {
        private final HashMap<String,Integer> wordCount = new HashMap<String,Integer>();

        private final boolean allLowercase = true;

        private final IntWritable one = new IntWritable(1); // stores the one value

        private final Set<String> ignorePatterns = new HashSet<String>();

        @Override
        public void setup(Reducer.Context context) throws IOException {
            Configuration conf = context.getConfiguration();

            // hardcoded for now
            ignorePatterns.add("\\.");
            ignorePatterns.add("\\,");
            ignorePatterns.add("!");

            URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();

            System.out.println("YOU ARE HERE" + patternsURIs.length);
//            for (URI patternsURI : patternsURIs) {
//                Path patternsPath = new Path(patternsURI.getPath());
//                String patternsFileName = patternsPath.getName().toString();
//
//                System.out.println("filename " + patternsFileName);
//                //parseSkipFile(patternsFileName);
//            }

//            String filename = "TEST";
//            updateWordCountFromFile(filename);
        }

        public void updateWordCountFromFile(String filename) {
            try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
                String line;
                // Iterate through the word count file and update the word count hashmap
                while ((line = br.readLine()) != null) {
                    System.out.println(line);

                    StringTokenizer itr = new StringTokenizer(line);

                    String word = itr.nextToken();
                    Integer count = Integer.valueOf(itr.nextToken());

                    System.out.println(word + " " + count);

                    // If the word already exists with a count in the hashmap, just update
                    // the count with the sum of the old count + the new one
                    if (wordCount.containsKey(word)) {
                        wordCount.put(word, wordCount.get(word) + count);
                    } else {
                        wordCount.put(word, count);
                    }
                }
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                System.out.println(key + " " + count + " " + value.toString());
                count++;
            }
            context.write(new Text("hello"), new Text("hello2"));

//            // Preprocess the sentence
//            String line;
//            if (allLowercase){
//                line = value.toString().toLowerCase();
//            } else {
//                line = value.toString();
//            }
//
//            for (String pattern : ignorePatterns) {
//                String oldline = line;
//                line = line.replaceAll(pattern, "");
//                //if (!oldline.equals(line)) {
//                //    System.out.println(oldline + " new: " + line);
//                //}
//            }
//
//            // Tokenize the sentence
//            StringTokenizer itr = new StringTokenizer(line);
//
//            // Iterate through all tokens and create a new Text, IntWritable tuple2
//            while (itr.hasMoreTokens()) {
//                Text current_word = new Text(itr.nextToken());
//                // currently, there is a count of one for this word
//                // this will tie in the reduce task
//                //System.out.println(current_word);
//                context.write(current_word, one);
//            }
//
//            int sum = 0;
//            for (IntWritable val : values) {
//                sum += val.get();
//            }
//            context.write(key, new IntWritable(sum));
        }
    }

    public static void run(String[] args) throws Exception {
        // Create Configuration and MR Job objects
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Score each sentence given a word count file");

        job.setJarByClass(SentenceScorer.class);

        job.setMapperClass(SentenceMapper.class);
        //job.setCombinerClass(ScorerReducer.class);
        job.setReducerClass(ScorerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        String localUserDirectory = System.getProperty("user.dir");
//        localUserDirectory = localUserDirectory.replace('\\','/');
//        System.out.println(localUserDirectory);

        job.addCacheFile(new URI ( "/TextSummary/test_word_count_map.txt"));

        // Delete output2 folder if it already exists on local file system
        // Remove if ready
        // LocalFileHelperAPI.DeleteDirectory(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        SentenceScorer.run(args);
    }
}
