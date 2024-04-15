import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class JoinMapMostCommonWords extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // super.map(key, value, context);
        StringTokenizer itr = new StringTokenizer(value.toString());
        String first_word = itr.nextToken();
        System.out.println("CWID : " + first_word + " and key" + key + " and value: " + value);
        context.write(new Text(first_word), value);
    }
}
