/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Proto;

import NameNode.DataNodeLocation;
import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;



/**
 *
 * @author saksham
 */
public class ProtoMessage {
    
    public static Hdfs.DataNodeLocation.Builder dataNodeLocation(String ip, Integer port) {
        Hdfs.DataNodeLocation.Builder builder = Hdfs.DataNodeLocation.newBuilder();
        builder.setIp(ip);
        builder.setPort(port);
        return builder;
    }
    
    public static byte[] openFileRequest(String filename, Boolean forRead) {
        Hdfs.OpenFileRequest.Builder builder = Hdfs.OpenFileRequest.newBuilder();
        builder.setFileName(filename);
        builder.setForRead(forRead);
        return builder.build().toByteArray();
    }
    
    public static byte[] openFileResponse(Integer status, Integer handle) {
        Hdfs.OpenFileResponse.Builder builder = Hdfs.OpenFileResponse.newBuilder();
        builder.setStatus(status);
        builder.setHandle(handle);
        return builder.build().toByteArray();
    }
    
    public static byte[] openFileResponse(Integer status, Integer handle, List<Integer> blocks) {
        Hdfs.OpenFileResponse.Builder builder = Hdfs.OpenFileResponse.newBuilder();
        builder.setStatus(status);
        builder.setHandle(handle);
        builder.addAllBlockNums(blocks);
        return builder.build().toByteArray();
    }
    
    public static byte[] closeFileRequest(Integer handle) {
        Hdfs.CloseFileRequest.Builder builder = Hdfs.CloseFileRequest.newBuilder();
        builder.setHandle(handle);
        return builder.build().toByteArray();
    }
    
    public static byte[] writeBlockRequest(byte[] data, Hdfs.BlockLocations blockLocations) {
        Hdfs.WriteBlockRequest.Builder builder = Hdfs.WriteBlockRequest.newBuilder();
        ByteString bs = ByteString.copyFrom(data);
        builder.addData(bs);
        builder.setBlockInfo(blockLocations);
        return builder.build().toByteArray();
    }
    
    //TODO: Remove this function and use the other writeBlockRequest instead.
    //Following the rule that use of builder has to be in this file (ProtoMessage) and this makes code simpler in DataNode
    public static byte[] writeBlockRequest(byte[] data, Integer blockNumber, ArrayList<Hdfs.DataNodeLocation> dataNodeLocations) {
        Hdfs.WriteBlockRequest.Builder builder = Hdfs.WriteBlockRequest.newBuilder();
        ByteString bs = ByteString.copyFrom(data);
        builder.addData(bs);
        Hdfs.BlockLocations.Builder blockLocationsBuilder = Hdfs.BlockLocations.newBuilder();
        for(Hdfs.DataNodeLocation dnl : dataNodeLocations) {
            blockLocationsBuilder.addLocations(dnl);
        }
        blockLocationsBuilder.setBlockNumber(blockNumber);
        builder.setBlockInfo(blockLocationsBuilder);
        return builder.build().toByteArray();
    }
    
    public static byte[] writeBlockResponse(Integer status) {
        Hdfs.WriteBlockResponse.Builder builder = Hdfs.WriteBlockResponse.newBuilder();
        builder.setStatus(status);
        return builder.build().toByteArray();
    }
    
    public static byte[] blockLocationRequest(List<Integer> blocks) {
        Hdfs.BlockLocationRequest.Builder builder = Hdfs.BlockLocationRequest.newBuilder();
        builder.addAllBlockNums(blocks);
        return builder.build().toByteArray();
    }
    
    public static byte[] blockLocationResponse(ArrayList<Integer> blocks, ArrayList<ArrayList<DataNodeLocation>> dnls) {
        Hdfs.BlockLocationResponse.Builder builder = Hdfs.BlockLocationResponse.newBuilder();
        for(int i=0; i<blocks.size(); i++) {
            Hdfs.BlockLocations.Builder locationBuilder = Hdfs.BlockLocations.newBuilder();
            locationBuilder.setBlockNumber(blocks.get(i));
            for (DataNodeLocation dnl: dnls.get(i)) {
                locationBuilder.addLocations(dnl.toProto());
            }
            builder.addBlockLocations(locationBuilder);
        }
        return builder.build().toByteArray();
    }
    
    public static byte[] readBlockRequest(Integer blockNum) {
        Hdfs.ReadBlockRequest.Builder builder = Hdfs.ReadBlockRequest.newBuilder();
        builder.setBlockNumber(blockNum);
        return builder.build().toByteArray();
    }
    
    public static byte[] readBlockResponse(byte[] data) {
        Hdfs.ReadBlockResponse.Builder builder = Hdfs.ReadBlockResponse.newBuilder();
        ByteString bs = ByteString.copyFrom(data);
        builder.addData(bs);
        return builder.build().toByteArray();
    }
            
    public static byte[] assignBlockRequest(Integer handle) {
        Hdfs.AssignBlockRequest.Builder builder = Hdfs.AssignBlockRequest.newBuilder();
        builder.setHandle(handle);
        return builder.build().toByteArray();
    }
    
    public static byte[] assignBlockResponse(Integer status, Integer blockNumber, ArrayList<String> ips, ArrayList<Integer> ports) {
        Hdfs.AssignBlockResponse.Builder builder = Hdfs.AssignBlockResponse.newBuilder();
        builder.setStatus(status);
        Hdfs.BlockLocations.Builder blockLocationsBuilder = Hdfs.BlockLocations.newBuilder();
        blockLocationsBuilder.setBlockNumber(blockNumber);
        Integer n = ports.size();
        for(Integer i=0; i<n; i++) {
            Hdfs.DataNodeLocation.Builder dataNodeLocationBuilder = Hdfs.DataNodeLocation.newBuilder();
            dataNodeLocationBuilder.setIp(ips.get(i));
            dataNodeLocationBuilder.setPort(ports.get(i));
            blockLocationsBuilder.addLocations(dataNodeLocationBuilder);
        }
        builder.setNewBlock(blockLocationsBuilder);
        return builder.build().toByteArray();
    }
    
    public static byte[] listFileResponse(Integer status, Set<String> fileNames) {
        Hdfs.ListFilesResponse.Builder builder = Hdfs.ListFilesResponse.newBuilder();
        builder.setStatus(status);
        builder.addAllFileNames(fileNames);
        return builder.build().toByteArray();
    }
    
    public static byte[] blockReportRequest(Integer dataNodeId, String ip, Integer port, Set<Integer> blockNumbers) {
        Hdfs.BlockReportRequest.Builder builder = Hdfs.BlockReportRequest.newBuilder();
        builder.setId(dataNodeId);
        builder.setLocation(dataNodeLocation(ip, port));
        builder.addAllBlockNumbers(blockNumbers);
        return builder.build().toByteArray();
    }
    
    public static byte[] blockReportResponse(ArrayList<Integer> responseStatuses) {
        Hdfs.BlockReportResponse.Builder builder = Hdfs.BlockReportResponse.newBuilder();
        builder.addAllStatus(responseStatuses);
        return builder.build().toByteArray();
    }
}
