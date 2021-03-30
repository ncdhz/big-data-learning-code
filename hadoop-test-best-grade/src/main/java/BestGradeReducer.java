import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BestGradeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int bestGrade = 0;
        for (IntWritable value: values) {
            int v = value.get();
            if (v > bestGrade) {
                bestGrade = v;
            }
        }
        context.write(new Text(key), new IntWritable(bestGrade));
    }
}
