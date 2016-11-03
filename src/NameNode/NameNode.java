/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;
import Proto.Hdfs;
import Proto.ProtoMessage;


import DataNode.IDataNode;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author saksham
 */

public class NameNode extends UnicastRemoteObject implements INameNode {

    private static final String DIRECTORY = "Data/NameNode/";
    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static final Integer REP_FACTOR = 2; //Replication Factor in DNs

    private static Integer DN_COUNT = -1;
    private static Integer globalBlockCounter = 0;
    private static Integer globalFileCounter = 0;
    private static String rmiHost = "";

    private static final HashMap<Integer, IDataNode> dns = new HashMap<>();
    private static final ArrayList<DataNodeLocation> dnLocations = new ArrayList<>();
    private static final HashMap<Integer, String> handleToOpenFileName = new HashMap<>();
    private static final HashMap<String, Integer> fileNameToHandle = new HashMap<>();
    private static final HashMap<Integer, ArrayList<Integer> > handleToBlocks = new HashMap<>();
    private static final HashMap<Integer, ArrayList<DataNodeLocation> > blockToDnLocations = new HashMap<>();
    
    NameNode() throws RemoteException {
        super();
    }
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[NameNode] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    private Integer createFile(String fileName) throws IOException {
        Integer handle = globalFileCounter;
        globalFileCounter++;
        handleToOpenFileName.put(handle, fileName);
        
        String blockFile = DIRECTORY + handle.toString() + ".txt";
        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(blockFile, true)));
        log("Writing to file " + fileName);
        pw.append(fileName + "\n");
        log("Writing " + fileName);
        pw.close();
        
        return handle;
    }
    
    private void addBlockToHandle(Integer handle, Integer blockNumber) throws IOException {
        if(handleToBlocks.containsKey(handle)) {
            handleToBlocks.get(handle).add(blockNumber);
        }
        else {
            ArrayList<Integer> l = new ArrayList<>();
            l.add(blockNumber);
            handleToBlocks.put(handle, l);
        }
        
        String fileName = DIRECTORY + handle.toString() + ".txt";
        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(fileName, true)));
        log("Writing to file " + fileName);
        pw.append(blockNumber.toString() + "\n");
        log("Writing " + blockNumber.toString());
        pw.close();
    }
    
    private void restoreStateFromDisk() throws IOException {
        log("Restoring state from disk");
        File dir = new File(DIRECTORY);
        File[] files = dir.listFiles();
        for (File file : files) {
            if (!file.isDirectory()) {
                String handleTxt = file.getName();
                log(handleTxt);
                Integer handle = Integer.parseInt(handleTxt.substring(0, handleTxt.length() - 4));
                globalFileCounter = Math.max(globalFileCounter, handle);

                List<String> lines = Files.readAllLines(Paths.get(file.getCanonicalPath()), StandardCharsets.UTF_8);
                
                Integer numberOfLines = lines.size();
                if(numberOfLines > 1) {
                    fileNameToHandle.put(lines.get(0), handle);
                    log("fileName : " + lines.get(0));
                    ArrayList<Integer> blocks = new ArrayList<>();
                    for(Integer i=1; i<numberOfLines; i++) {
                        Integer blockNum = Integer.parseInt(lines.get(i));
                        blocks.add(blockNum);
                        globalBlockCounter = Math.max(globalBlockCounter, blockNum);
                    }
                    log("blocks : " + blocks.toString());
                    handleToBlocks.put(handle, blocks);
                }
            }
        }
        globalFileCounter++;
        globalBlockCounter++;
    }
    
    private void addDnLocationToBlock(Integer blockNumber, DataNodeLocation dnl) {
        if(blockToDnLocations.containsKey(blockNumber)) {
            blockToDnLocations.get(blockNumber).add(dnl);
        }
        else {
            ArrayList<DataNodeLocation> l = new ArrayList<>();
            l.add(dnl);
            blockToDnLocations.put(blockNumber, l);
        }
    }
    
    public static void main(String args[]) {
        Properties props = new Properties();
        try {
            props.load(new BufferedReader(new FileReader("config.properties")));
        } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        rmiHost = props.getProperty("rmiserver.host", "localhost")
                + ":" + props.getProperty("rmiserver.port", "1099");

        try {
            NameNode nn = new NameNode();
            nn.restoreStateFromDisk();
            log(fileNameToHandle.toString());
            log(handleToBlocks.toString());
            DN_COUNT = Integer.parseInt(args[1]);
            
            Naming.rebind("rmi://" + rmiHost + "/"+NN_NAME, nn);
            log("Bound to RMI");
            nn.finddns(DN_COUNT);
        } catch (Exception e) { log(e.toString()); }
        for(;;) {}
    }

    @Override
        public byte[] openFile(byte[] inp) throws RemoteException {
            Hdfs.OpenFileRequest openFileRequest;
            try {
                openFileRequest = Hdfs.OpenFileRequest.parseFrom(inp);
                if(openFileRequest.getForRead() == false) {
                    log("Opening file '"
                            + openFileRequest.getFileName()
                            + "' for Writing with handle "
                            + globalFileCounter.toString());
                    Integer handle = createFile(openFileRequest.getFileName());
                    // ^ TODO Check for errors while creating file and send response accordingly
                    byte[] openFileResponse = ProtoMessage.openFileResponse(1, handle);
                    return openFileResponse;
                }
                else {
                    String filename = openFileRequest.getFileName();
                    if(fileNameToHandle.containsKey(filename) == false)
                        return ProtoMessage.openFileResponse(0, -1);
                    Integer handle = fileNameToHandle.get(filename);
                    log("Opening file '"
                            + openFileRequest.getFileName()
                            + "' for Reading with handle "
                            + handle.toString()
                            + " and assigned blocks as "
                            + handleToBlocks.get(handle).toString());
                    return ProtoMessage.openFileResponse(1, handle, handleToBlocks.get(handle));
                }
            } catch (InvalidProtocolBufferException ex) {
                Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
            }
            return null;
        }

    @Override
        public byte[] closeFile(byte[] inp) throws RemoteException {
            Hdfs.CloseFileRequest closeFileRequest = null;
            Integer status = 0;
            try {
                closeFileRequest = Hdfs.CloseFileRequest.parseFrom(inp);
                Integer handle = closeFileRequest.getHandle();
                if(handleToOpenFileName.containsKey(handle)) {
                    String fileName = handleToOpenFileName.get(handle);
                    fileNameToHandle.put(fileName, handle);
                    handleToOpenFileName.remove(handle);
                    status = 1;
                }
            } catch (InvalidProtocolBufferException e) {log(e.toString());}
            return ProtoMessage.closeFileResponse(status);
        }

    @Override
        public byte[] getBlockLocations(byte[] inp) throws RemoteException {
            Hdfs.BlockLocationRequest request = null;
            try {
                request = Hdfs.BlockLocationRequest.parseFrom(inp);
            } catch (Exception e) { log(e.toString()); }
            ArrayList<Integer> blockNums = new ArrayList<>(request.getBlockNumsList());
            ArrayList<ArrayList<DataNodeLocation>> dnls = new ArrayList<>();
            for (Integer block: blockNums) {
                dnls.add(blockToDnLocations.get(block));
            }
            return ProtoMessage.blockLocationResponse(blockNums, dnls);
        }

    @Override
        public byte[] assignBlock(byte[] inp) throws RemoteException {
            Hdfs.AssignBlockRequest assignBlockRequest;
            byte[] ret = null;
            try {
                assignBlockRequest = Hdfs.AssignBlockRequest.parseFrom(inp);
                Integer handle = assignBlockRequest.getHandle();
                addBlockToHandle(handle, globalBlockCounter);
                ArrayList<String> ips = new ArrayList<>();
                ArrayList<Integer> ports = new ArrayList<>();
                for(int i=0; i<REP_FACTOR; i++)
                {
                    Random rand = new Random();
                    Integer dataNodeId = rand.nextInt(DN_COUNT);
                    DataNodeLocation dnl = dnLocations.get(dataNodeId);
                    if (ports.contains(dnl.port)) {
                        i--;
                    } else {
                        ips.add(dnl.ip);
                        ports.add(dnl.port);
                        addDnLocationToBlock(globalBlockCounter, dnl);
                    }
                }
                log("Handle " + handle.toString()
                        + " assigned Block " + globalBlockCounter.toString()
                        + " assigned DNs: " + ports.toString());

                ret = ProtoMessage.assignBlockResponse(1, globalBlockCounter, ips, ports);
                globalBlockCounter++;
            } catch (InvalidProtocolBufferException ex) {
                Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
            }
            return ret;
        }

    @Override
        public byte[] list(byte[] inp) throws RemoteException {
            Set<String> keys = fileNameToHandle.keySet();
            return ProtoMessage.listFileResponse(1, keys);
        }

    @Override
        public byte[] blockReport(byte[] inp) throws RemoteException {
            // TODO Lock on blockToDnLocations
            ArrayList<Integer> responseStatuses = new ArrayList<>();
            try {
                Hdfs.BlockReportRequest blockReportRequest = Hdfs.BlockReportRequest.parseFrom(inp);
                DataNodeLocation dnl = new DataNodeLocation(blockReportRequest.getLocation().getIp(), blockReportRequest.getLocation().getPort());
                ArrayList<Integer> blockNumbers = new ArrayList<Integer>(blockReportRequest.getBlockNumbersList());
                log("[BlockReport] received from : " + blockReportRequest.getLocation().getPort());
                for(Integer blockNumber : blockNumbers) {
                    log("[BlockReport] BlockNumber : " + blockNumber);
                    if(blockToDnLocations.containsKey(blockNumber) == false) {
                        log("[BlockReport] blockNumber not found in blockToDnLocations");
                        addDnLocationToBlock(blockNumber, dnl);
                        log("[BlockReport] adding");
                        responseStatuses.add(1);
                    }
                    else {
                        ArrayList<DataNodeLocation> dnls = blockToDnLocations.get(blockNumber);
                        Boolean contains = false;
                        for(DataNodeLocation x : dnls) {
                            if(dnl.port == x.port) {
                                contains = true;
                                break;
                            }
                        }
                        if(contains == false) {
                            log("[BlockReport] DnLocation not found in blockToDnLocations");
                            addDnLocationToBlock(blockNumber, dnl);
                            log("[BlockReport] adding");
                            responseStatuses.add(1);
                        }
                        else {
                            log("[BlockReport] All OK. Already exists");
                            responseStatuses.add(1);
                        }
                    }
                }
            } catch (Exception e) {log(e.toString());}
            return ProtoMessage.blockReportResponse(responseStatuses);
        }

    @Override
        public byte[] heartBeat(byte[] inp) throws RemoteException {
            return null;
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
        for(Integer i=0; i<DN_COUNT; i++)
            dnLocations.add(new DataNodeLocation("", i));
    }
}

