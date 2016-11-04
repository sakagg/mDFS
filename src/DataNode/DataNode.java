/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataNode;

import NameNode.INameNode;
import NameNode.NameNode;
import Proto.Hdfs;
import Proto.ProtoMessage;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
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
    private static String Directory;
    private static Properties props = new Properties();
    
    private INameNode nn = null;
    
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
        
        Directory = DIRECTORY_PREFIX + myId + "/";
        
        try {
            props.load(new BufferedReader(new FileReader("config.properties")));
        } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        try {
            LocateRegistry.createRegistry(Integer.parseInt(props.getProperty("rmi.datanode.port")));
        } catch (RemoteException ex) {}
        
        DataNode dn;
        try {
            dn = new DataNode();
            Naming.rebind("rmi://localhost:" + props.getProperty("rmi.datanode.port") + "/" + DN_PREFIX, dn);
            log("Bound to RMI");
            dn.findnn();
            dn.reportIP();
            dn.reportBlocks();
        } catch (RemoteException | MalformedURLException ex) {
            Logger.getLogger(DataNode.class.getName()).log(Level.SEVERE, null, ex);
        }
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
                IDataNode dn;
                try {
                    dn = (IDataNode) Naming.lookup("rmi://" + blockLocations.getLocations(1).getIp()
                            + ":" + blockLocations.getLocations(1).getPort()
                            + "/" + DN_PREFIX);
                    log("Cascading request for blockNumber " + blockNumber + " to datanode: " + blockLocations.getLocations(1).getPort());
                    dn.writeBlock(casecadedwriteBlockRequest);
                } catch (NotBoundException ex) {
                    Logger.getLogger(DataNode.class.getName()).log(Level.SEVERE, null, ex);
                } catch (MalformedURLException ex) {
                    Logger.getLogger(DataNode.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            return ProtoMessage.writeBlockResponse(1);
        }
    
    public void reportIP() {
        byte[] request = ProtoMessage.reportIPRequest(myId, props.getProperty("rmi.datanode.ip"), Integer.parseInt(props.getProperty("rmi.datanode.port")));
        try {
            nn.reportIP(request);
        } catch (RemoteException ex) {
            Logger.getLogger(DataNode.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void reportBlocks() {
        List<Integer> blockNumbers = getAllBlocks();
        String ip = props.getProperty("rmi.datanode.ip");
        Integer port = Integer.parseInt(props.getProperty("rmi.datanode.port"));
        byte[] req = ProtoMessage.blockReportRequest(myId, ip, port, blockNumbers);
        
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
        
    public void findnn() {
        while(nn == null)
        {
            try {
                nn = (INameNode) Naming.lookup("rmi://" + props.getProperty("rmi.namenode.ip")
                        + ":" + props.getProperty("rmi.namenode.port") + "/" + NN_NAME);
                log("Found Name Node");
            } catch (Exception e) {}
            if (nn == null)
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
        }
    }
    
}
