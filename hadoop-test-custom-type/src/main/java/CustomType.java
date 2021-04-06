import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class CustomType {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job customTypeJob = Job.getInstance(conf, "custom type");

        // 指定 job 所在的类
        customTypeJob.setJarByClass(CustomType.class);

        // 设置mapper逻辑类为那个类
        customTypeJob.setMapperClass(CustomTypeMapper.class);
        // 设置reducer逻辑类为那个类
        customTypeJob.setReducerClass(CustomTypeReducer.class);

        // 设置map阶段输出的kv数据类型
        customTypeJob.setMapOutputKeyClass(MemberLogTime.class);
        customTypeJob.setMapOutputValueClass(IntWritable.class);

        // 设置reducer阶段输出的kv数据类型
        customTypeJob.setOutputKeyClass(MemberLogTime.class);
        customTypeJob.setOutputValueClass(IntWritable.class);

        customTypeJob.setSortComparatorClass(MemberLogTimeComparator.class);
        // 输入文本所在路径
        FileInputFormat.setInputPaths(customTypeJob, new Path(otherArgs[0]));
        // 输出文本所在路径
        FileOutputFormat.setOutputPath(customTypeJob, new Path(otherArgs[1]));
        // 提交job给hadoop集群
        customTypeJob.waitForCompletion(true);
    }
}
