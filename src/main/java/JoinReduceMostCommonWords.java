import com.fasterxml.jackson.databind.util.TypeKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

public class JoinReduceMostCommonWords extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //super.reduce(key, values, context);
//        System.out.println("reduce:" + key);
//        for (Text rec : values) {
//            System.out.println("it: " + rec);
//        }

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

//        String studentRec = "";
//        List<String> courseList = new ArrayList();
//        String cwid= "";
//        //
//        for (Text rec : values) {
//            StringTokenizer itr = new StringTokenizer(rec.toString());
//            cwid = itr.nextToken();
//            String fInd = itr.nextToken();
//            if (fInd.indexOf("CPSC") == 0) {
//                courseList.add(rec.toString());
//            } else {
//                studentRec = rec.toString();
//            }
//        }
//        //
//        for (String c : courseList) {
//            String value = studentRec + " " + c;
//            context.write(new Text(cwid), new Text(value));
//        }
        //StringTokenizer itr = new StringTokenizer(rec.toString());
        //cwid = itr.nextToken();
        //context.write(new Text("The"), new Text("Hello World!"));
    }

}
