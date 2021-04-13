import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FriendRecommendationMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] friends = data.split(" ");
        if(friends.length >= 2) {
            context.write(new Text(friends[0]), new Text(friends[1]));
            context.write(new Text(friends[1]), new Text(friends[0]));
        }
    }
}
