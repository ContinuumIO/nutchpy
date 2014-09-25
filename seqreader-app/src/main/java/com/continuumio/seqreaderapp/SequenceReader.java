package com.continuumio.seqreaderapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.scoring.webgraph.LinkDatum;
import org.apache.nutch.util.NutchConfiguration;
import py4j.GatewayServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;


public class SequenceReader {
    private int nrows = 5;

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

        List<List> rows=new ArrayList<List>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

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

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

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

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        Writable value = (Writable)
                ReflectionUtils.newInstance(reader.getValueClass(), conf);


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
            i += 1;
        }

        return rows;
    }

    public static long count(String path) throws IOException  {
        //read rows between start and stop

         Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);
        System.out.println(file);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

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


