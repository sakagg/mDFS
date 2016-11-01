/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataNode;

import NameNode.INameNode;
import Proto.Hdfs;
import Proto.ProtoMessage;
import java.io.File;
import java.io.FilenameFilter;
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
import java.util.Set;

/**
 *
 * @author saksham
 */
public class DataNode extends UnicastRemoteObject implements IDataNode {
    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static Integer myId = -1;
    private static Integer DN_COUNT = -1;
    private INameNode nn = null;
    private static final HashMap<Integer, IDataNode> dns = new HashMap<>();
    private static HashMap<Integer, byte[]> chunks = new HashMap<>();
    
    private static final FilenameFilter textFilter = new FilenameFilter() {
        public boolean accept(File dir, String name) {
            return name.toLowerCase().startsWith(myId + "_") && name.toLowerCase().endsWith(".txt");
        }
    };
    
    DataNode() throws RemoteException {
        super();
    }
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[DataNode" + myId.toString() + "] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    public void persistWriteBlock(Integer blockNumber, byte[] data) throws IOException {
        String fileName = "Compiled/DataNode/" + myId + "_" + blockNumber.toString() + ".txt";
        Path path = Paths.get(fileName);
        log("Writing to file " + fileName);
        Files.write(path, data);
        log("Writing " + blockNumber.toString());
    }
    
    private void restoreStateFromDisk() throws IOException {
        log("persisting state from disk");
        File dir = new File("Compiled/DataNode/");
        File[] files = dir.listFiles(textFilter);
        for (File file : files) {
            if (!file.isDirectory()) {
                String myId_blockTxt = file.getName();
                log(myId_blockTxt);
                Integer blockNumber = Integer.parseInt(myId_blockTxt.substring(myId_blockTxt.indexOf("_")+1, myId_blockTxt.length() - 4));
                log("blockNumber " + blockNumber);
                List<String> lines = Files.readAllLines(Paths.get(file.getCanonicalPath()), StandardCharsets.UTF_8);
                
                Integer numberOfLines = lines.size();
                if(numberOfLines == 1) {
                    chunks.put(blockNumber, lines.get(0).getBytes());
                    log("contents : " + lines.get(0));
                }
            }
        }
    }
    
    public static void main (String args[]) {
        myId = Integer.parseInt(args[1]);
        DN_COUNT = Integer.parseInt(args[3]);
        try {
            DataNode dn = new DataNode();
            dn.restoreStateFromDisk();
            Naming.rebind("rmi://localhost/" + DN_PREFIX + myId.toString(), dn);
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
            try {
                Hdfs.ReadBlockRequest readBlockRequest = Hdfs.ReadBlockRequest.parseFrom(inp);
                block = readBlockRequest.getBlockNumber();
            } catch (Exception e) { log(e.toString()); }
            log("Block " + block.toString() + " has contents: " + new String(chunks.get(block), StandardCharsets.UTF_8));
            return ProtoMessage.readBlockResponse(chunks.get(block));
        }
	
    @Override
        public byte[] writeBlock(byte[] inp) throws RemoteException {
            Hdfs.WriteBlockRequest writeBlockRequest = null;
            try {
                writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);
            } catch (Exception e) { log(e.toString()); }
            byte[] data = writeBlockRequest.getData(0).toByteArray();
            Integer blockNumber = writeBlockRequest.getBlockInfo().getBlockNumber();
            chunks.put(blockNumber, data);
            try {
                persistWriteBlock(blockNumber, data);
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
        Set<Integer> blockNumbers = new HashSet<Integer> (chunks.keySet());
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
                    dn = (IDataNode) Naming.lookup("rmi://localhost/" + DN_PREFIX + i.toString());
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
                nn = (INameNode) Naming.lookup("rmi://localhost/" + NN_NAME);
                log("Found Name Node");
            } catch (Exception e) {}
            if (nn == null)
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
        }
    }
    
}
