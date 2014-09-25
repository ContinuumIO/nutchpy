package com.continuumio.seqreaderapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.scoring.webgraph.LinkDatum;
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class OutlinkReader {

    public static HashMap<String, String> getOutlinksRow(Writable key, LinkDatum value) {
        HashMap<String, String> t_row = new HashMap<String, String>();
        t_row.put("key_url", key.toString());
        t_row.put("url", value.getUrl());
        t_row.put("anchor", value.getAnchor());
        t_row.put("score", String.valueOf(value.getScore()));
        t_row.put("timestamp", String.valueOf(value.getTimestamp()));
        t_row.put("linktype", String.valueOf(value.getLinkType()));

        return t_row;
    }


    public static List read(String path) throws IOException {
        // reads the entire contents of the file

        List<HashMap> rows=new ArrayList<HashMap>();

        Configuration conf = NutchConfiguration.create();
        FileSystem fs = FileSystem.get(conf);

        Path file = new Path(path);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        LinkDatum value = new LinkDatum();

        while(reader.next(key, value)) {
            try {
                HashMap<String, String> t_row = getOutlinksRow(key,value);
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

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        LinkDatum value = new LinkDatum();

        int i = 0;
        while(reader.next(key, value)) {

            if (i == nrows) {
                break;
            }
            i += 1;
            try {
                HashMap<String, String> t_row = getOutlinksRow(key,value);
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

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, file, conf);

        Writable key = (Writable)
                ReflectionUtils.newInstance(reader.getKeyClass(), conf);
        LinkDatum value = new LinkDatum();

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
                HashMap<String, String> t_row = getOutlinksRow(key,value);
                rows.add(t_row);

            }
            catch (Exception e) {
            }
        }

        return rows;
    }


}
