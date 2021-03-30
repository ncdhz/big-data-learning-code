import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HDFSWriteFile {

    public static void main(String[] args) throws IOException {
        /**
         * args[0] 文件名
         * args[1] 内容
         */
        writeFile(args[0], args[1].getBytes());
    }

    private static void writeFile(String fileName, byte[] data) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path dstPath = new Path(fileName);
        FSDataOutputStream outputStream = fs.create(dstPath);
        try {
            outputStream.write(data);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            outputStream.close();
            fs.close();
            System.out.println("File write success");
        }
    }
}
