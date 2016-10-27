/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Proto;

import com.google.protobuf.ByteString;
import java.util.ArrayList;



/**
 *
 * @author saksham
 */
public class ProtoMessage {
    
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
        Integer n = ips.size();
        for(Integer i=0; i<n; i++) {
            Hdfs.DataNodeLocation.Builder dataNodeLocationBuilder = Hdfs.DataNodeLocation.newBuilder();
            dataNodeLocationBuilder.setIp(ips.get(i));
            dataNodeLocationBuilder.setPort(ports.get(i));
            blockLocationsBuilder.addLocations(dataNodeLocationBuilder);
        }
        return builder.build().toByteArray();
    }
}
