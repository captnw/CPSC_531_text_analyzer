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
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.javatuples.Pair;
import org.slf4j.LoggerFactory;


public class SentenceScorer {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(SentenceMapper.class);

    public static class SentenceMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, Integer> wordCount = new HashMap<>();

        private Configuration conf;

        @Override
        protected void setup(Mapper.Context context) throws IOException {
            conf = context.getConfiguration();
            URI[] cached_files = Job.getInstance(conf).getCacheFiles();

            // There should only be one cached file (which is used to share the wordCount file with the map job)
            if (cached_files.length != 1) {
                throw new IllegalArgumentException("Sentence scorer's 4th argument expects only one file");
            }

            URI wordCountURI = cached_files[0];
            Path wordCountFilePath = new Path(wordCountURI.getPath());
            String wordCountFileName = wordCountFilePath.toString(); // should be "output/part-r-00000"

            // Read word count from the part-r-00000 file
            try (BufferedReader br = new BufferedReader(new FileReader(wordCountFileName))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\\s+");
                    wordCount.put(parts[0], Integer.parseInt(parts[1]));
                }
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
            //System.out.println(score + sentence);
            LOG.info(sentence);
            // Show sentence with score
            context.write( new Text(String.valueOf(score)), new Text(sentence));
            //random uuid to give a sentence a key
            //context.write(new Text(UUID.randomUUID().toString()), value);
        }
    }

    public static class ScorerReducer extends Reducer<Text, Text, Text, Text> {
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
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                // While we want the sentences in our summary, keep them in
                // At this point, the keys should be sorted, so we are processing in descending
                // order
                if(sentenceCount < topNSentence) {
                    context.write(key, value);
                    sentenceCount++;
                }
                break;
            }
        }
    }

    public static Pair<Boolean, Double> run(String[] args) throws Exception {
        // Create Configuration and MR Job objects
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Score each sentence given a word count file");

        job.setJarByClass(SentenceScorer.class);

        job.setMapperClass(SentenceMapper.class);
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
        job.addCacheFile(wordCountFile); // Used in mapping
        // Use configuration in reduce task
        job.getConfiguration().setInt("sentencescorer.reduce.numSentencesInSummary",numSentencesInSummary);

        // Measure how long the job takes
        Long startTime = System.nanoTime();
        Boolean jobStatus = job.waitForCompletion(true);
        Long endTime = System.nanoTime();
        // Calculate time diff and convert nanoseconds to seconds
        Double elapsedTime = (endTime - startTime)/1e9d;

        return new Pair<Boolean, Double>(jobStatus, elapsedTime);
    }

    public static void main(String[] args) throws Exception {
        Pair<Boolean, Double> status = SentenceScorer.run(args);
        System.out.println("This job took " + status.getValue1() + " seconds");

        System.exit(status.getValue0() ? 0 : 1);
    }
}
