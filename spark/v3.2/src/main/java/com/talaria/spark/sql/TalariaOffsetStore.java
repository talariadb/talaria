package com.talaria.spark.sql;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;

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
        TalariaOffset offset = null;
        if(fs.exists(path)){
            try (FSDataInputStream in = fs.open(path)) {
                byte[] buf = in.readAllBytes();
                offset = TalariaOffset.fromJSON(new String(buf, StandardCharsets.UTF_8));
            } catch (IOException exception) {
                // log exception
            }
        }
        else {
            try (FSDataOutputStream out = fs.create(path, true)) {
                offset = new TalariaOffset(epoch);
                out.write(offset.json().getBytes(StandardCharsets.UTF_8));
            } catch (IOException exception){
                // log exception
            }
        }
        return offset;
    }

    public void updateOffset(TalariaOffset offset) throws IOException {
        try (FSDataOutputStream out = fs.create(path, true)) {
            epoch = offset.getOffset();
            out.write(offset.json().getBytes(StandardCharsets.UTF_8));
        } catch (IOException exception) {
            // log error for exception
        }
    }
}