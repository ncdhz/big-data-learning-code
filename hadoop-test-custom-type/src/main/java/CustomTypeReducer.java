import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CustomTypeReducer extends Reducer<MemberLogTime, IntWritable, MemberLogTime, IntWritable> {
    @Override
    protected void reduce(MemberLogTime key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable value: values) {
            sum += value.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
