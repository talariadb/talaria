package com.talaria.spark.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class TalariaOffsetStore {
    private final Path path;
    private Long epoch;
    private final FileSystem fs;

    TalariaOffsetStore(Configuration conf, String location, Long epoch) throws IOException {
        this.fs = FileSystem.get(URI.create(location), conf);
        this.path = new Path(URI.create(location));
        this.epoch = epoch;
    }

    public TalariaOffset initialOffset() throws IOException {
        TalariaOffset offset;
        if(fs.exists(path)){
            FSDataInputStream in = fs.open(path);
            byte[] buf = in.readAllBytes();
            offset = TalariaOffset.fromJSON(new String(buf, StandardCharsets.UTF_8));
            in.close();
        }
        else {
            offset = new TalariaOffset(epoch);
            FSDataOutputStream out = fs.create(path, true);
            out.write(offset.json().getBytes(StandardCharsets.UTF_8));
            out.close();
        }
        return offset;
    }

    public void updateOffset(TalariaOffset offset) throws IOException {
        epoch = offset.getOffset();
        FSDataOutputStream out = fs.create(path, true);
        out.write(offset.json().getBytes(StandardCharsets.UTF_8));
        out.close();
    }
}