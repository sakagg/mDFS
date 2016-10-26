/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;
import Proto.Hdfs.*;
import Proto.ProtoMessage;


import static Client.Client.log;
import DataNode.IDataNode;
import com.google.protobuf.InvalidProtocolBufferException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 *
 * @author saksham
 */

class DataNodeLocation {
    String ip;
    Integer port;

    public DataNodeLocation(String i, Integer p) {
        ip = i;
        port = p;
    }
}

public class NameNode implements INameNode {

    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static final Integer DN_COUNT = 1;

    int globalBlockCounter;
    int globalFileCounter;

    private static final HashMap<Integer, IDataNode> dns = new HashMap<>();
    private static final LinkedList<DataNodeLocation> dnLocations = new LinkedList<>();
    private static final HashMap<String, Integer> fileNameToHandle = new HashMap<>();
    private static final HashMap<Integer, LinkedList<Integer> > handleToBlocks = new HashMap<>();
    private static final HashMap<Integer, LinkedList<DataNodeLocation> > blockToDnLocations = new HashMap<>();

    NameNode() throws RemoteException {
        super();
        globalBlockCounter = 0; // TODO initialise from fsimage
        globalFileCounter = 0; // TODO initialise from fsimage
        finddns(DN_COUNT);
    }
    
    private void addBlockToHandle(Integer handle, Integer blockNumber) {
        if(handleToBlocks.containsKey(handle)) {
            handleToBlocks.get(handle).add(blockNumber);
        }
        else {
            LinkedList<Integer> l = new LinkedList<>();
            handleToBlocks.put(handle, l);
        }
    }
    
    private void addDnLocationToBlock(Integer blockNumber, DataNodeLocation dnl) {
        if(blockToDnLocations.containsKey(blockNumber)) {
            blockToDnLocations.get(blockNumber).add(dnl);
        }
        else {
            LinkedList<DataNodeLocation> l = new LinkedList<>();
            blockToDnLocations.put(blockNumber, l);
        }
    }
    
    public static void main(String args[]) throws MalformedURLException {
        try {
            DataNodeLocation dnl = new DataNodeLocation("127.0.0.1",1099);
            dnLocations.add(dnl);
            
            LocateRegistry.createRegistry(1099);
            NameNode nn = new NameNode();
            Naming.rebind("//localhost/RmiServer"+NN_NAME, nn);
        } catch (RemoteException e) { }
    }

    @Override
        public byte[] openFile(byte[] inp) throws RemoteException {
            OpenFileRequest openFileRequest;
            try {
                openFileRequest = OpenFileRequest.parseFrom(inp);
                if(openFileRequest.getForRead() == false) {
                    fileNameToHandle.put(openFileRequest.getFileName(), globalFileCounter);
                    byte[] openFileResponse = ProtoMessage.openFileResponse(1,globalFileCounter);
                    globalFileCounter++;
                    return openFileResponse;
                }
                else {

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
            return null;
        }

    @Override
        public byte[] assignBlock(byte[] inp) throws RemoteException {
        AssignBlockRequest assignBlockRequest;
        try {
            assignBlockRequest = AssignBlockRequest.parseFrom(inp);
            Integer handle = assignBlockRequest.getHandle();
            Random rand = new Random();
            Integer dataNodeId = rand.nextInt(DN_COUNT);
            DataNodeLocation dnl = dnLocations.get(dataNodeId);
            LinkedList<String> ips = new LinkedList<>();
            LinkedList<Integer> ports = new LinkedList<>();
            ips.add(dnl.ip);
            ports.add(dnl.port);
            // TODO add in memory
            addBlockToHandle(handle, globalBlockCounter);
            addDnLocationToBlock(globalBlockCounter, dnl);
           
            byte[] ret = ProtoMessage.assignBlockResponse(1,globalBlockCounter,ips,ports);
            globalBlockCounter++;
            return ret;
            // currently giving only 1 blockLocation
        } catch (InvalidProtocolBufferException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
            return null;
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
    }
}

