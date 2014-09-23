package com.continuumio.seqreaderapp;


import org.apache.hadoop.io.*;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import py4j.GatewayServer;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Iterator;

public class App {
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

    public static List head(int nrows, String path) throws IOException  {
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

        for(int i = 0; i < nrows; i = i+1) {
            reader.next(key, value);
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

        //reader.seek(start);
        for(long i = 0; i < start; i = i+1) {
            reader.next(key,value);
        }

        for(long i = start; i < stop; i = i+1) {
            reader.next(key, value);
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


    public static void write(HashMap hashMap, String filepath) throws IOException  {
        // Write dictionary/HashMap to Sequencefile
        System.out.println(filepath);

//        Writable key = (Writable)
//                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
//        Writable value = (Writable)
//                ReflectionUtils.newInstance(reader.getValueClass(), conf);



        String uri = filepath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Writer writer = null;

        System.out.println(hashMap.getClass().toString());

        Iterator keySetIterator = hashMap.keySet().iterator();



        keySetIterator.next();

        // get general key/value object
        Object key = keySetIterator.next();
        Object value = hashMap.get(key);
        System.out.println(key.getClass().toString());
        System.out.println(value.getClass().toString());

        //detect object type
        Object key_class = type_to_writable(key.getClass());
        Object value_class = type_to_writable(value.getClass());

        System.out.println(key_class.getClass().toString());
        System.out.println(value_class.getClass().toString());

        writer = SequenceFile.createWriter(fs, conf, path,
                key_class.getClass(), value_class.getClass());

//        key_class.getClass() keyText = (key_class.getClass()) key;

//        writer.append(keyText, value);

        try {
            while (keySetIterator.hasNext()) {
                key = keySetIterator.next();
                System.out.println("key: " + key.toString() + " value: " + hashMap.get(key).toString());

                writer.append(key, value);

            }
        } finally {
                IOUtils.closeStream(writer);
        }

    }



    public static void main(String[] args) {
//        SequenceWriter sequenceWriter = new SequenceWriter();
//        try {
////            sequenceWriter.write_seq();
//        } catch (Exception e) {
//            System.out.println("ERROR!!!!!");
//        }

        int port;
        boolean dieOnBrokenPipe = false;
        String usage = "usage: [--die-on-bro1ken-pipe] port";
        if (args.length == 0) {
            System.err.println(usage);
            System.exit(1);
        } else if (args.length == 2) {
            if (!args[0].equals("--die-on-broken-pipe")) {
                System.err.println(usage);
                System.exit(1);
            }
            dieOnBrokenPipe = true;
        }
        port = Integer.parseInt(args[args.length - 1]);
        GatewayServer server = new GatewayServer(new App(), port);
        server.start();
        int listening_port = server.getListeningPort();
        System.out.println("" + listening_port);
        if (dieOnBrokenPipe) {
            /* Exit on EOF or broken pipe.  This ensures that the server dies
             * if its parent program dies. */
            BufferedReader stdin = new BufferedReader(
                    new InputStreamReader(System.in));
            try {
                stdin.readLine();
                System.exit(0);
            } catch (java.io.IOException e) {
                System.exit(1);
            }
        }
    }
}

