import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class CustomTypeMapper extends Mapper<LongWritable, Text, MemberLogTime, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split(",");
        String name = data[0];
        String time = data[1];
        String[] strings = time.split("-");
        context.write(new MemberLogTime(name, strings[0]+"-"+strings[1]), new IntWritable(1));
    }
}
