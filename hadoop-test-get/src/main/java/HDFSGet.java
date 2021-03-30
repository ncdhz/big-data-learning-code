import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSGet {
    public static void main(String[] args) throws IOException {
        /**
         * args[0] 服务器文件
         * args[1] 本地文件路径
         */
        String hdfsPath = args[0];
        String localPath = args[0];
        get(hdfsPath, localPath);
    }

    private static void get(String hdfsPath, String localPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path hp = new Path(hdfsPath);
        Path lp = new Path(localPath);
        try {
            fs.copyToLocalFile(hp, lp);
            System.out.print("Copy to local success!\n");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            fs.close();
        }
    }
}
