import org.apache.hadoop.io.Writable;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


import py4j.GatewayServer;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;

public class SeqReaderApp {
    private int nrows = 5;


    public static List head(int nrows, String path) throws IOException  {

        List<String> rows=new ArrayList<String>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);
        
        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
            ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
            ReflectionUtils.newInstance(reader.getValueClass(), conf);

        for(int i = 0; i < nrows; i = i+1) {
            reader.next(key, value);
            try {
                rows.add(key + ": " + value.toString());
            }
            catch (Exception e) {
            }
        }

        return rows;
    }

    public static List slice(long start, long stop, String path) throws IOException  {

        List<String> rows=new ArrayList<String>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);
        
        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
            ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
            ReflectionUtils.newInstance(reader.getValueClass(), conf);
	
        //reader.seek(start);
        for(long i = 0; i < start; i = i+1) {
           reader.next(key,value);
        }	
        for(long i = start; i < stop; i = i+1) {
            reader.next(key, value);
            try {
                rows.add(key + ": " + value.toString());
            }
            catch (Exception e) {
            }
        }

        return rows;
    }

    public static void main(String[] args) {
        
        SeqReaderApp app = new SeqReaderApp();
        // app is now the gateway.entry_point
        GatewayServer server = new GatewayServer(app);
        server.start();

    }

}

