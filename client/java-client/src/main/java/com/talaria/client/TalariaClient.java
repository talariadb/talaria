package com.talaria.client;

import com.google.protobuf.ByteString;
import com.talaria.protos.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class TalariaClient {
    // use logger.
    private static final Logger logger = Logger.getLogger(TalariaClient.class.getName());
    private final QueryGrpc.QueryBlockingStub blockingStub;
    // make use of asyncStub whereever possible.
    private final QueryGrpc.QueryStub asyncStub;
    private final ManagedChannel channel;

    public TalariaClient(String host, int port) {
        channel = ManagedChannelBuilder.forTarget(String.format("%s:%d", host, port)).usePlaintext().build();
        blockingStub = QueryGrpc.newBlockingStub(channel);
        asyncStub = QueryGrpc.newStub(channel);
    }

    public TalariaClient(String host, int port, int maxInboundMsgSize) {
        channel = ManagedChannelBuilder.forTarget(String.format("%s:%d", host, port)).maxInboundMessageSize(maxInboundMsgSize).usePlaintext().build();
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

    public GetRowsResponse getRows(ByteString splitId, List<String> cols) {
        GetRowsRequest req = GetRowsRequest.newBuilder().setSplitID(splitId).addAllColumns(cols).build();
        return blockingStub.getRows(req);
    }

    public GetRowsResponse getRows(ByteString splitId, List<String> cols, int maxMsgSize) {
        GetRowsRequest req = GetRowsRequest.newBuilder().setSplitID(splitId).addAllColumns(cols).setMaxBytes(maxMsgSize).build();
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

    public List<Endpoint> getNodes() {
        GetNodesRequest req = GetNodesRequest.newBuilder().build();
        GetNodesResponse resp = blockingStub.getNodes(req);
        return resp.getNodesList();
    }
}