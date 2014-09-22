package com.continuumio.seqreaderapp;


import org.apache.hadoop.io.Writable;
import org.apache.nutch.util.NutchConfiguration;
import org.apache.hadoop.util.ReflectionUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import py4j.GatewayServer;

import java.util.List;
import java.util.ArrayList;
import java.io.IOException;


public class App {
    private int nrows = 5;


    public static List head(int nrows, String path) throws IOException  {

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

    public static List slice(long start, long stop, String path) throws IOException  {

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

    public static void main(String[] args) {
        int port;
        boolean dieOnBrokenPipe = false;
        String usage = "usage: [--die-on-broken-pipe] port";
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

