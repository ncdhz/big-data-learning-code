import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class WordCountComparator extends WritableComparator {
    protected WordCountComparator() {
        super(IntWritable.class, true);
    }
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntWritable x = (IntWritable) a;
        IntWritable y = (IntWritable) b;
        return -1 * x.compareTo(y);
    }
}
