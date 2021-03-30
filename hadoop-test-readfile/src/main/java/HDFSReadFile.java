import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class HDFSReadFile {

    public static void main(String[] args) throws IOException {
        readFile(args[0]);
    }

    private static void readFile(String filePath) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs= FileSystem.get(conf);
        Path srcPath = new Path(filePath);
        InputStream in = null;
        try {
            in = fs.open(srcPath);
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            IOUtils.closeStream(in);
        }
    }
}
