package com.continuumio.seqreaderapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.nutch.scoring.webgraph.Node;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class NodeReader {
    private int nrows = 5;

    public static HashMap<String, String> getNodeRow(Writable key, Node value) {
        HashMap<String, String> t_row = new HashMap<String, String>();
        t_row.put("key_url", key.toString());
        t_row.put("num_inlinks", String.valueOf(value.getNumInlinks()) );
        t_row.put("num_outlinks", String.valueOf(value.getNumOutlinks()) );
        t_row.put("inlink_score", String.valueOf(value.getInlinkScore()));
        t_row.put("outlink_score", String.valueOf(value.getOutlinkScore()));
        t_row.put("metadata", value.getMetadata().toString());

        return t_row;
    }


    public static List read(String path) throws IOException {
        // reads the entire contents of the file

        List<HashMap> rows=new ArrayList<HashMap>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Node value = new Node();

        while(reader.next(key, value)) {
            try {
                HashMap<String, String> t_row = getNodeRow(key,value);
                rows.add(t_row);
            }
            catch (Exception e) {
            }
        }

        return rows;
    }

    public static List head(int nrows, String path) throws IOException {
        // reads the entire contents of the file

        List<HashMap> rows=new ArrayList<HashMap>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Node value = new Node();

        int i = 0;
        while(reader.next(key, value)) {

            if (i == nrows) {
                break;
            }
            i += 1;
            try {
                HashMap<String, String> t_row = getNodeRow(key,value);
                rows.add(t_row);
            }
            catch (Exception e) {
            }
        }

        return rows;
    }

    public static List slice(long start, long stop, String path) throws IOException  {
        // reads the entire contents of the file

        List<HashMap> rows=new ArrayList<HashMap>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);

        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(file));

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Node value = new Node();

        //skip rows
        long i = 0;
        while(reader.next(key, value)) {
            if (i == start) {
                break;
            }
            i += 1;
        }

        while(reader.next(key, value)) {
            if (i == stop) {
                break;
            }

            i += 1;

            try {
                HashMap<String, String> t_row = getNodeRow(key,value);
                rows.add(t_row);

            }
            catch (Exception e) {
            }
        }

        return rows;
    }


    public static long count(String path) throws IOException  {
        //read rows between start and stop

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

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
