import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class BestGrade {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job bestGradeJob = Job.getInstance(conf, "word count");

        // 指定 job 所在的类
        bestGradeJob.setJarByClass(BestGrade.class);

        // 设置mapper逻辑类为那个类
        bestGradeJob.setMapperClass(BestGradeMapper.class);
        // 设置reducer逻辑类为那个类
        bestGradeJob.setReducerClass(BestGradeReducer.class);

        // 设置map阶段输出的kv数据类型
        bestGradeJob.setMapOutputKeyClass(Text.class);
        bestGradeJob.setMapOutputValueClass(IntWritable.class);

        // 设置reducer阶段输出的kv数据类型
        bestGradeJob.setOutputKeyClass(Text.class);
        bestGradeJob.setOutputValueClass(IntWritable.class);

        // 输入文本所在路径
        FileInputFormat.setInputPaths(bestGradeJob, new Path(otherArgs[0]));
        // 输出文本所在路径
        FileOutputFormat.setOutputPath(bestGradeJob, new Path(otherArgs[1]));
        // 提交job给hadoop集群
        bestGradeJob.waitForCompletion(true);
    }
}
