package com.continuumio.seqreaderapp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.nutch.util.NutchConfiguration;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 *
 * This iterator reads the sequence file as and when the records are consumed.
 *
 */
public class RecordIterator implements Iterator<List<Writable>> {

     private static final Configuration CONFIG = NutchConfiguration.create();
    private SequenceFile.Reader reader;
    private Writable key;
    private Writable value;
    private List<Writable> next;
    private long numRecordsRead;
    private long limit;


    public RecordIterator(SequenceFile.Reader reader) throws IOException {
        this(reader, 0, Long.MAX_VALUE);
    }

    /**
     * Creates iterator for sequence file records
     * @param reader The sequence file reader
     * @param start The starting record number
     * @param limit number of records to read
     */
    public RecordIterator(SequenceFile.Reader reader, long start,
                          long limit) throws IOException {

        this.reader = reader;
        this.key = (Writable) ReflectionUtils.newInstance(
                                                reader.getKeyClass(), CONFIG);
        this.value = (Writable) ReflectionUtils.newInstance(
                                                reader.getValueClass(), CONFIG);
        this.limit = limit;

        //skip rows till the start position ;
        for(int i = 0; i < start && reader.next(key, value); i++);
        this.next = readNext();
    }

    public boolean hasNext() {
        return next != null;
    }


    /**
     * gets Number of records read from the reader
     * @return number of records read
     */
    public long getNumRecordsRead() {
        return numRecordsRead;
    }

    private List<Writable> readNext() {
        if (reader == null) {
            throw new IllegalStateException("Reader is null");
        }
        try {
            if (numRecordsRead < limit && reader.next(key, value)) {
                numRecordsRead++;
                return Arrays.asList(key, value);
            } else {
                //end of the reader or limit reached, close it and exit
                reader.close();
                reader = null;
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public List<Writable> next() {
        List<Writable> temp = next;
        next = readNext();
        return temp;
    }

    /* JDK 8 has this as default implementation, but we need this for
     * backward compatibility.
     */
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
