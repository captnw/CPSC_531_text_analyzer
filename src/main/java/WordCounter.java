import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordCounter {
    public static class JoinMapMostCommonWords extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            // super.map(key, value, context);
            StringTokenizer itr = new StringTokenizer(value.toString());
            String first_word = itr.nextToken();
            System.out.println("CWID : " + first_word + " and key" + key + " and value: " + value);
            context.write(new Text(first_word), value);
        }
    }

    public static class JoinReduceMostCommonWords extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Hashmap ... iterate over a sentence, for each word, if ti doesn't exist
            // in hashmap, initialize key value where value is 0 and key is word
            // otherwise increment value in hashmap
            HashMap<String, Integer> wordCount = new HashMap<String, Integer>();

            for (Text sentence : values) {
                StringTokenizer tokenizer = new StringTokenizer(sentence.toString());
                while (tokenizer.hasMoreTokens()) {
                    String word = tokenizer.nextToken();
                    Integer count = wordCount.get(word);
                    if (count == null) {
                        wordCount.put(word, 1);
                    } else {
                        wordCount.put(word, ++count);
                    }
                }
            }

            // Iterate through hashmap and write out the values
            // word value
//        wordCount.forEach(
//                (key2, value) -> System.out.println(key2 + " " + value)
//        );

            for (Map.Entry<String, Integer> entry : wordCount.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(entry.getValue() + ""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        // Create Configuration and MR Job objects
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Join Student and Course");

        job.setJarByClass(WordCounter.class);

        job.setMapperClass(JoinMapMostCommonWords.class);
        job.setReducerClass(JoinReduceMostCommonWords.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //job.setInputFormatClass(TextInputFormat.class);
        //job.setOutputFormatClass(TextOutputFormat.class);

        // Delete output2 folder if it already exists on local file system
        // Remove if ready
        LocalFileHelperAPI.DeleteDirectory(args[1]);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
