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
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author saksham
 */

public class NameNode extends UnicastRemoteObject implements INameNode {

    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static Integer DN_COUNT = -1;
    private static final Integer REP_FACTOR = 2; //Replication Factor in DNs

    Integer globalBlockCounter = 0;
    Integer globalFileCounter = 0;

    private static final HashMap<Integer, IDataNode> dns = new HashMap<>();
    private static final ArrayList<DataNodeLocation> dnLocations = new ArrayList<>();
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
    
    private void addBlockToHandle(Integer handle, Integer blockNumber) {
        if(handleToBlocks.containsKey(handle)) {
            handleToBlocks.get(handle).add(blockNumber);
        }
        else {
            ArrayList<Integer> l = new ArrayList<>();
            l.add(blockNumber);
            handleToBlocks.put(handle, l);
        }
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
        DN_COUNT = Integer.parseInt(args[1]);
        try {
            LocateRegistry.createRegistry(1099);
            log("Started Registry");
        } catch (Exception e) { log(e.toString()); }
        try {
            NameNode nn = new NameNode();
            Naming.rebind("rmi://localhost/"+NN_NAME, nn);
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
                    fileNameToHandle.put(openFileRequest.getFileName(), globalFileCounter);
                    byte[] openFileResponse = ProtoMessage.openFileResponse(1, globalFileCounter);
                    globalFileCounter++;
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
            }
            return null;
        }

    @Override
        public byte[] closeFile(byte[] inp) throws RemoteException {
            return null;
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
                        // TODO add in memory
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
            }
            return ret;
        }

    @Override
        public byte[] list(byte[] inp) throws RemoteException {
            return null;
        }

    @Override
        public byte[] blockReport(byte[] inp) throws RemoteException {
            return null;
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
        for(Integer i=0; i<DN_COUNT; i++)
            dnLocations.add(new DataNodeLocation("", i));
    }
}

