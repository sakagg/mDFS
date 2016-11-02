/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataNode;

import NameNode.INameNode;
import NameNode.NameNode;
import static NameNode.NameNode.log;
import Proto.Hdfs;
import Proto.ProtoMessage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author saksham
 */
public class DataNode extends UnicastRemoteObject implements IDataNode {
    
    private static final String DIRECTORY_PREFIX = "Data/DataNode/";
    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    
    private static Integer myId = -1;
    private static Integer DN_COUNT = -1;
    private static String Directory;
    private static String rmiHost = "";
    
    private INameNode nn = null;
    private static final HashMap<Integer, IDataNode> dns = new HashMap<>();
    
    DataNode() throws RemoteException {
        super();
    }
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[DataNode" + myId.toString() + "] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    public void putBlock(Integer blockNumber, byte[] data) throws IOException {
        String fileName = Directory + blockNumber.toString();
        Path path = Paths.get(fileName);
        log("Writing to file " + fileName);
        Files.write(path, data);
        log("Writing " + blockNumber.toString());
    }
    
    private byte[] getBlock(Integer blockNumber) throws IOException {
        String filename = Directory + blockNumber.toString();
        Path path = Paths.get(filename);
        return Files.readAllBytes(path);
    }
    
    private ArrayList<Integer> getAllBlocks() {
        File dir = new File(Directory);
        File[] files = dir.listFiles();
        ArrayList<Integer> blockNums = new ArrayList<>(); 
        for (File file: files) {
            if (!file.isDirectory()) {
                String filename = file.getName();
                Integer blockNum = Integer.parseInt(filename);
                blockNums.add(blockNum);
            }
        }
        return blockNums;
    }
    
    public static void main (String args[]) {
        myId = Integer.parseInt(args[1]);
        DN_COUNT = Integer.parseInt(args[3]);
        
        Directory = DIRECTORY_PREFIX + myId + "/";
        
        Properties props = new Properties();
        try {
            props.load(new BufferedReader(new FileReader("config.properties")));
        } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        rmiHost = props.getProperty("rmiserver.host", "localhost")
                + ":" + props.getProperty("rmiserver.port", "1099");

        try {
            DataNode dn = new DataNode();
            Naming.rebind("rmi://" + rmiHost + "/" + DN_PREFIX + myId.toString(), dn);
            log("Bound to RMI");
            dn.finddns(DN_COUNT);
            dn.findnn();
            dn.reportBlocks();
        } catch (Exception e) { log(e.toString()); }
        for(;;) {}
    }
    
    @Override
        public byte[] readBlock(byte[] inp) throws RemoteException {
            Integer block = -1;
            byte[] data = null;
            try {
                Hdfs.ReadBlockRequest readBlockRequest = Hdfs.ReadBlockRequest.parseFrom(inp);
                block = readBlockRequest.getBlockNumber();
                data = getBlock(block);
            } catch (Exception e) { log(e.toString()); }
            log("Block " + block.toString() + " has contents: " + new String(data, StandardCharsets.UTF_8));
            return ProtoMessage.readBlockResponse(data);
        }
	
    @Override
        public byte[] writeBlock(byte[] inp) throws RemoteException {
            Hdfs.WriteBlockRequest writeBlockRequest = null;
            try {
                writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);
            } catch (Exception e) { log(e.toString()); }
            byte[] data = writeBlockRequest.getData(0).toByteArray();
            Integer blockNumber = writeBlockRequest.getBlockInfo().getBlockNumber();
            try {
                putBlock(blockNumber, data);
            } catch (IOException e) {log(e.toString());} // TODO return status 0
            log(new String(data, StandardCharsets.UTF_8));
            
            Hdfs.BlockLocations blockLocations = null;
            blockLocations = writeBlockRequest.getBlockInfo();
            
            Integer numberofLocations = blockLocations.getLocationsCount();
            log("numberofBlockLocations : " + numberofLocations);
            if(numberofLocations > 1) {
                ArrayList<Hdfs.DataNodeLocation> dataNodeLocations = new ArrayList<>();
                for(Integer i=0; i<numberofLocations; i++)
                    dataNodeLocations.add(blockLocations.getLocations(i));
                dataNodeLocations.remove(0);
                blockNumber = blockLocations.getBlockNumber();
                byte[] casecadedwriteBlockRequest = ProtoMessage.writeBlockRequest(data, blockNumber, dataNodeLocations);
                IDataNode dn = dns.get(blockLocations.getLocations(1).getPort());
                log("Cascading request for blockNumber " + blockNumber + " to datanode: " + blockLocations.getLocations(1).getPort());
                dn.writeBlock(casecadedwriteBlockRequest);
            }
            return ProtoMessage.writeBlockResponse(1);
        }
    
    public void reportBlocks() {
        List<Integer> blockNumbers = getAllBlocks();
        byte[] req = ProtoMessage.blockReportRequest(myId, "", myId, blockNumbers);
        
        try {
            byte[] res = nn.blockReport(req);
            log("[BlockReport] Sending : " + blockNumbers);
            Hdfs.BlockReportResponse blockReportResponse = Hdfs.BlockReportResponse.parseFrom(res);
            ArrayList<Integer> responseStatuses = new ArrayList<Integer> (blockReportResponse.getStatusList());
            Integer i = 0;
            for(Integer blockNumber : blockNumbers) {
                Integer responseStatus = responseStatuses.get(i);
                i++;
                log("[BlockReport] response status of " + blockNumber + " : " + responseStatus);
            }
        } catch (Exception e) { log(e.toString()); }
    }    
        
    public void finddns(Integer numberDNs) {
        HashSet<Integer> leftPeers = new HashSet<>();
        for(int i=0; i<numberDNs; i++)
            leftPeers.add(i);
        for(;;) {
            ArrayList<Integer> toDelete = new ArrayList<>();
            for(Integer i: leftPeers) {
                IDataNode dn;
                try {
                    dn = (IDataNode) Naming.lookup("rmi://" + rmiHost + "/" + DN_PREFIX + i.toString());
                } catch (Exception e) {
                    continue;
                }
                toDelete.add(i);
                dns.put(i, dn);
                log("Found Data Node " + i.toString());
            }
            toDelete.stream().forEach((i) -> {leftPeers.remove(i);});
            if(leftPeers.isEmpty()) {
                break;
            }
            try {
                Thread.sleep(1000);
            } catch (Exception E) {}
        }
    }
    
    public void findnn() {
        while(nn == null)
        {
            try {
                nn = (INameNode) Naming.lookup("rmi://" + rmiHost + "/" + NN_NAME);
                log("Found Name Node");
            } catch (Exception e) {}
            if (nn == null)
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
        }
    }
    
}
