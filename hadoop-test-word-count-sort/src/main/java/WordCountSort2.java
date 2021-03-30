import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class WordCountSort2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job wordCountSortJob = Job.getInstance(conf, "word count sort");

        // 指定 job 所在的类
        wordCountSortJob.setJarByClass(WordCountSort2.class);

        // 设置mapper逻辑类为那个类
        wordCountSortJob.setMapperClass(WordCountSortMapper.class);
        // 设置reducer逻辑类为那个类
        wordCountSortJob.setReducerClass(WordCountSortReducer.class);

        // 设置map阶段输出的kv数据类型
        wordCountSortJob.setMapOutputKeyClass(IntWritable.class);
        wordCountSortJob.setMapOutputValueClass(Text.class);

        // 设置reducer阶段输出的kv数据类型
        wordCountSortJob.setOutputKeyClass(Text.class);
        wordCountSortJob.setOutputValueClass(IntWritable.class);

        wordCountSortJob.setSortComparatorClass(WordCountComparator.class);
        // 输入文本所在路径
        FileInputFormat.setInputPaths(wordCountSortJob, new Path(otherArgs[0]));
        // 输出文本所在路径
        FileOutputFormat.setOutputPath(wordCountSortJob, new Path(otherArgs[1]));
        // 提交job给hadoop集群
        wordCountSortJob.waitForCompletion(true);
    }
}
