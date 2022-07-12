package com.talaria.client;

import com.google.protobuf.ByteString;
import com.talaria.protos.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TalariaClient {
    private static final Logger logger = Logger.getLogger(TalariaClient.class.getName());
    private final QueryGrpc.QueryBlockingStub blockingStub;
    private final QueryGrpc.QueryStub asyncStub;
    private final ManagedChannel channel;

    public TalariaClient(String host, int port) {
        channel = ManagedChannelBuilder.forTarget(host+":"+ port).usePlaintext().build();
        blockingStub = QueryGrpc.newBlockingStub(channel);
        asyncStub = QueryGrpc.newStub(channel);
    }

    public void close() {
        channel.shutdownNow();
    }

    public TableMeta getTableMeta(String name) {
        DescribeTableRequest req = DescribeTableRequest.newBuilder().setName(name).build();
        return blockingStub.describeTable(req).getTable();
    }

    public void listTables() {
        DescribeRequest req = DescribeRequest.newBuilder().build();
        DescribeResponse resp = blockingStub.describe(req);
        System.out.println(resp);
    }


    public GetRowsResponse getRows(ByteString splitId, List<String> cols) {
        GetRowsRequest req = GetRowsRequest.newBuilder().setSplitID(splitId).addAllColumns(cols).setMaxBytes(4096*1000).build();
        return blockingStub.getRows(req);
    }

    public List<ByteString> getSplits(String schema, String tableName, String... filters){
        GetSplitsRequest req = GetSplitsRequest.newBuilder()
                .setSchema(schema)
                .setTable(tableName)
                .addAllFilters(Arrays.asList(filters))
                .build();
        GetSplitsResponse resp = blockingStub.getSplits(req);
        return resp.getSplitsList().stream().map(Split::getSplitID).collect(Collectors.toList());
    }




    public static void main(String[] args) throws InterruptedException {
        TalariaClient client = null;
        try {
            client = new TalariaClient("127.0.0.1", 8043);
           List<ByteString> r = client.getSplits("events", "event == 'relay.outcome'");

             List<String> cols = new ArrayList<>(Collections.emptyList());
             cols.add("event");
             cols.add("ingested_at");
             cols.add("ingested_by");
             GetRowsResponse re = client.getRows(r.get(0), cols);
             System.out.println(re.getNextToken().isEmpty());
             System.out.println(re.getRowCount());
             System.out.println(re);
        } finally {
            assert client != null;
            client.close();
        }
    }
}