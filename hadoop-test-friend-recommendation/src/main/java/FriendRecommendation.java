import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class FriendRecommendation {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Job friendRecommendationJob = Job.getInstance(conf, "friend recommendation");

        // 指定 job 所在的类
        friendRecommendationJob.setJarByClass(FriendRecommendation.class);

        // 设置mapper逻辑类为那个类
        friendRecommendationJob.setMapperClass(FriendRecommendationMapper.class);
        // 设置reducer逻辑类为那个类
        friendRecommendationJob.setReducerClass(FriendRecommendationReducer.class);

        // 设置map阶段输出的kv数据类型
        friendRecommendationJob.setMapOutputKeyClass(Text.class);
        friendRecommendationJob.setMapOutputValueClass(Text.class);

        // 设置reducer阶段输出的kv数据类型
        friendRecommendationJob.setOutputKeyClass(Text.class);
        friendRecommendationJob.setOutputValueClass(Text.class);

        // 输入文本所在路径
        FileInputFormat.setInputPaths(friendRecommendationJob, new Path(otherArgs[0]));
        // 输出文本所在路径
        FileOutputFormat.setOutputPath(friendRecommendationJob, new Path(otherArgs[1]));
        // 提交job给hadoop集群
        friendRecommendationJob.waitForCompletion(true);    }
}
