/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;

import static Client.Client.log;
import DataNode.IDataNode;
import com.sun.javafx.font.freetype.HBGlyphLayout;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;


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

    private HashMap<Integer, IDataNode> dns = new HashMap<>();

    private HashMap<String, Integer> fileNameToNumber = new HashMap<>();
    private HashMap<Integer, LinkedList<Integer> > fileNumberToBlocks = new HashMap<>();
    private HashMap<Integer, LinkedList<DataNodeLocation> > blockToDnLocations = new HashMap<>();

    NameNode() throws RemoteException {
        super();
        globalBlockCounter = 0; // TODO initialise from fsimage
        globalFileCounter = 0; // TODO initialise from fsimage
        finddns(DN_COUNT);
    }

    public static void main(String args[]) throws MalformedURLException {
        try {
            LocateRegistry.createRegistry(1099);
            NameNode nn = new NameNode();
            Naming.rebind("//localhost/RmiServer"+NN_NAME, nn);
        } catch (RemoteException e) { }
    }

    @Override
        public byte[] openFile(byte[] inp) throws RemoteException {
            OpenFileRequest ofr = ProtoHelper.getOpenFileRequest(inp);
            
            if(ofr.getForRead() == false) {
                fileNameToNumber.put(ofr.getFileName(), globalFileCounter);

                globalFileCounter++;
            }
            else {

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

