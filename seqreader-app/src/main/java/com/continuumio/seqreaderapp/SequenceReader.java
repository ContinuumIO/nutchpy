package com.continuumio.seqreaderapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class SequenceReader {
    private static final Configuration conf = NutchConfiguration.create();
    private static final FileSystem fs;
    static {
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //convert Java Types to Hadoop Writable Types
    public static Object type_to_writable(Object obj) {
        Object new_obj;

        System.out.println(obj.toString());
        System.out.println(obj == String.class);

        if (obj.equals(String.class)){
            new_obj = new Text();
        }
        else if (obj.equals(Integer.class)) {
            new_obj = new IntWritable();
        }
        else {
            new_obj = new Object();
        }
        return new_obj;
    }

    public static List head(int nrows, String path) throws IOException {
        // read first nrows from sequence file

        List<List> rows = new ArrayList<List>();

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
                ReflectionUtils.newInstance(reader.getValueClass(), conf);

        int i = 0;
        while(reader.next(key, value)) {
            if (i == nrows) {
                break;
            }
            try {

                //hack JAVA tuple construction
                //need to keep types simple for python conversion
                List<String> t_row =new ArrayList<String>();

                t_row.add(key.toString());
                t_row.add(value.toString());
                rows.add(t_row);
            }  catch (Exception e) {
                //figure out something to do
                // how to pass messags back upstream to python
            }
            i += 1;

        }

        return rows;
    }

    public static List read(String path) throws IOException  {
        // reads the entire contents of the file

        List<List> rows=new ArrayList<List>();
        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
                ReflectionUtils.newInstance(reader.getValueClass(), conf);

        while(reader.next(key, value)) {
            try {

                //hack JAVA tuple construction
                //need to keep types simple for python conversion
                List<String> t_row =new ArrayList<String>();

                t_row.add(key.toString());
                t_row.add(value.toString());
                rows.add(t_row);
            }
            catch (Exception e) {
            }
        }

        return rows;
    }

    public static List slice(long start, long stop, String path) throws IOException  {
        //read rows between start and stop

        List<List> rows=new ArrayList<List>();

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
                ReflectionUtils.newInstance(reader.getValueClass(), conf);

        //skip rows
        long i = 0;
        for (; i < start && reader.next(key, value); i++ ); //Skip
        for (; i < stop && reader.next(key, value); i++ ){
            try {
                //hack JAVA tuple construction
                //need to keep types simple for python conversion
                List<String> t_row =new ArrayList<String>();
                t_row.add(key.toString());
                t_row.add(value.toString());
                rows.add(t_row);
            }
            catch (Exception e) {
                e.printStackTrace();
            }
        }
        return rows;
    }

    /**
     * Lazily reads the sequence file as and when the records are consumed from
     * the returned iterator
     * @param path path of sequence file
     * @param start starting record number
     * @return Iterator of Writable list which stores key and value in first two indices
     * @throws IOException when an error occurs
     */
    public static RecordIterator getRecordIterator(String path, long start)
            throws IOException {
        return getRecordIterator(path, start, Long.MAX_VALUE);
    }

    /**
     * Lazily reads the sequence file as and when the records are consumed from
     * the returned iterator
     * @param path - sequence file path
     * @param start - the starting record number
     * @param limit - number of records to read
     * @return Iterator of Writable list which stores key and value in first two indices
     * @throws IOException when an error occurs
     */
    public static RecordIterator getRecordIterator(
            String path, long start, long limit) throws IOException{

        Path file = new Path(path);
        System.out.println("Creating document stream from : " + file);
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));
        return new RecordIterator(reader, start, limit);
    }

    public static long count(String path) throws IOException  {
        //read rows between start and stop

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
                ReflectionUtils.newInstance(reader.getValueClass(), conf);

        //skip rows
        long i = 0;
        while(reader.next(key, value)) {
            i += 1;
        }
        return i;
    }
}
