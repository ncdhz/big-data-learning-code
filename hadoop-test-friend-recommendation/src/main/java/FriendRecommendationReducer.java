import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class FriendRecommendationReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value: values) {
            String name = value.toString();
            Set<String> ls = new HashSet<String>();
            for (Text v1: values) {
                String friend = v1.toString();
                if (! friend.equals(name)) {
                    ls.add(friend);
                }
            }
            if (!ls.isEmpty()) {
                context.write(new Text(name), new Text(String.join("\t", ls)));
            }
        }
    }
}
