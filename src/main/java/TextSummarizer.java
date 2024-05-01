import org.javatuples.Pair;

public class TextSummarizer {
    public static void main(String[] args) throws Exception {
        String inputFileLocation = args[0];
        String wordCounterOutputLocation = args[1];
        String sentenceScoreOutputLocation = args[2];
        String wordCounterPartRFile = args[3];
        String numSentencesKeep = args[4];

        String[] wordCounterArguments = new String[]{inputFileLocation, wordCounterOutputLocation};

        Pair<Boolean, Double> wordCountJobStatus = WordCounter.run(wordCounterArguments);
        Pair<Boolean, Double> sentenceScorerStatus = new Pair<>(false, 0D);
        if (wordCountJobStatus.getValue0()) {
            String[] sentenceScorerArguments = new String[]{inputFileLocation,
                                                            sentenceScoreOutputLocation,
                                                            wordCounterPartRFile,
                                                            numSentencesKeep};
            sentenceScorerStatus = SentenceScorer.run(sentenceScorerArguments);

            System.out.println("Word count job finished in " + wordCountJobStatus.getValue1() + " seconds");
        }
        System.out.println("Jobs finished");

        if (sentenceScorerStatus.getValue0()) {
            System.out.println("Sentence scorer job finished in " + sentenceScorerStatus.getValue1() + " seconds");
        }

        System.exit((wordCountJobStatus.getValue0() && sentenceScorerStatus.getValue0()) ? 0 : 1);
    }
}
