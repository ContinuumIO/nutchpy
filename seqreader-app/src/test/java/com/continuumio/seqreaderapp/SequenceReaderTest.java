package com.continuumio.seqreaderapp;

import junit.framework.TestCase;

import java.io.File;
import java.util.List;

/**
 * Created by tg on 9/25/15.
 */
public class SequenceReaderTest extends TestCase {

    private static File testFile = new File("../nutchpy/ex_data/crawldb_data");
    private static String testFilePath = testFile.getAbsolutePath();
    public static final int NUM_TEST_RECS = 8;

    public void testSlice() throws Exception {
        List slice = SequenceReader.slice(0, 2, testFilePath);
        assertTrue(slice.size() == 2);
        List head = SequenceReader.head(2, testFilePath);
        assertEquals(head.get(0), slice.get(0));
    }
}