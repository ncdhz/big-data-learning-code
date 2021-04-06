import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class MemberLogTimePartitioner extends Partitioner<MemberLogTime, IntWritable> {
    @Override
    public int getPartition(MemberLogTime logTime, IntWritable text, int i) {
        String data = logTime.getLogTime();
        int month = Integer.parseInt(data.substring(5));
        return month % i;
    }
}
