import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSPut {
    public static void main(String[] args) throws IOException {
        /**
         * args[0] 本地文件
         * args[1] 服务器文件目录
         */
        String localPath = args[0];
        String hdfsPath = args[1];
        put(localPath, hdfsPath);
    }

    private static void put(String localPath, String hdfsPath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path lp = new Path(localPath);
        Path hp = new Path(hdfsPath);
        try {
            fs.copyFromLocalFile(lp, hp);
            System.out.print("Copy to hadoop success!\n");
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            fs.close();
        }
    }

}
