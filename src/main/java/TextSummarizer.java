import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.ToolRunner;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class TextSummarizer {
    public static void main(String[] args) throws Exception {
        // Parse the input and place them into their respective arguments list
        Configuration conf = new Configuration();
        GenericOptionsParser parser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = parser.getRemainingArgs();
        if ((remainingArgs.length == 1 && Objects.equals(remainingArgs[0], "help")) || remainingArgs.length != 5) {
            System.err.println("Usage: TextSummarizer <input path> <wordcount output path> <sentence score output path> <wordcount file> <num sentences to display>");
        }

        List<String> wordCounterArguments = new ArrayList<>();
        List<String> sentenceScorerArguments = new ArrayList<>();

        for (int i = 0; i < remainingArgs.length; i++) {
            String arg = remainingArgs[i];
            switch (i) {
                case 0:
                    wordCounterArguments.add(arg);
                    sentenceScorerArguments.add(arg);
                    break;
                case 1:
                    wordCounterArguments.add(arg);
                    break;
                case 2:
                case 3:
                case 4:
                    sentenceScorerArguments.add(arg);
                    break;
            }
        }

        String[] wordCounterArgumentsTemp = wordCounterArguments.toArray(new String[0]);

        // Invoke wordCount after arguments are ready to be passed in
        int wordCounterExitCode = ToolRunner.run(conf, new WordCounter(), wordCounterArgumentsTemp);
        int sentenceScorerExitCode = 0;
        if (wordCounterExitCode == 0) {
            String[] sentenceScorerArgumentsTemp = sentenceScorerArguments.toArray(new String[0]);
            sentenceScorerExitCode = ToolRunner.run(conf, new SentenceScorer(), sentenceScorerArgumentsTemp);
        }
        System.out.println("Jobs finished");

        System.exit((wordCounterExitCode == 0 && sentenceScorerExitCode == 0) ? 0 : 1);
    }
}
