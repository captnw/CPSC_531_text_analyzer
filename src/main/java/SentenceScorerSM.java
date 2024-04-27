import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import java.util.Map;
import java.util.UUID;

public class SentenceScorerSM {
    public static class SentenceMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //random uuid to give a sentence a key
            context.write(new Text(UUID.randomUUID().toString()), value);
        }
    }

    public static class ScorerReducer extends Reducer<Text, Text, Text, Text> {
        private final Map<String, Integer> wordCount = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // Read word count from the part-r-00000 file
            try (BufferedReader br = new BufferedReader(new FileReader("part-r-00000"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    wordCount.put(parts[0], Integer.parseInt(parts[1]));
                }
            }
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int score = 0;
            String sentence = "";
            for (Text value : values) {
                sentence = value.toString();
                String[] words = sentence.split("\\s+");
                for (String word : words) {
                    // Lookup word count and accumulate score
                    if (wordCount.containsKey(word)) {
                        score += wordCount.get(word);
                    }
                }
            }
            // Show sentence with score
            context.write(new Text(sentence), new Text(Integer.toString(score)));
        }
    }

    public static void run(String[] args) throws Exception {
        // Create Configuration and MR Job objects
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Score each sentence given a word count file");

        job.setJarByClass(SentenceScorerSM.class);

        job.setMapperClass(SentenceMapper.class);
        job.setReducerClass(ScorerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        Path outputDir = new Path(args[1]);
        // Construct the path to part-r-00000 file relative to the output directory
        Path partFilePath = new Path(outputDir, "part-r-00000");

        //path to part-r-00000 is pass through config file
        //its the 3rd arguement
        job.addCacheFile(new Path(args[2]).toUri());
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        SentenceScorerSM.run(args);
    }
}
