/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Client;

import NameNode.INameNode;
import DataNode.IDataNode;
import Proto.Hdfs;
import Proto.ProtoMessage;
import com.google.protobuf.ByteString;
import java.rmi.Naming;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Scanner;

/**
 *
 * @author saksham
 */
public class Client {
    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static final Integer CHUNK_SIZE = 10;
    private static Integer DN_COUNT = -1;
    
    private INameNode nn = null;
    private HashMap<Integer, IDataNode> dns = new HashMap<>();
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[Client] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    public static void main(String args[]) {
        DN_COUNT = Integer.parseInt(args[1]);
        Client client = new Client();
        client.findnn();
        client.finddns(DN_COUNT);
        client.mainloop();
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
    
    public Integer openFileForWrite(String filename) {
        byte[] openRequest = ProtoMessage.openFileRequest(filename, Boolean.FALSE);
        Integer handle = -1;
        try {
            byte[] openResponse = nn.openFile(openRequest);
            handle = Hdfs.OpenFileResponse.parseFrom(openResponse).getHandle();
        } catch (Exception e) {}
        return handle;
    }
    
    public List<Hdfs.BlockLocations> openFileForRead(String fileName) {
        byte[] openRequest = ProtoMessage.openFileRequest(fileName, Boolean.TRUE);
        List<Hdfs.BlockLocations> blockLocations = null;
        try {
            byte[] res = nn.openFile(openRequest);
            Hdfs.OpenFileResponse openResponse = Hdfs.OpenFileResponse.parseFrom(res);
            byte[] blockLocationRequest = ProtoMessage.blockLocationRequest(openResponse.getBlockNumsList());
            byte[] blockLocationResponse = nn.getBlockLocations(blockLocationRequest);
            Hdfs.BlockLocationResponse response = Hdfs.BlockLocationResponse.parseFrom(blockLocationResponse);
            blockLocations = response.getBlockLocationsList();
        } catch (Exception e) { log(e.toString()); }
        return blockLocations;
    }
    
    public void closeFile(Integer handle) {
        byte[] closeRequest = ProtoMessage.closeFileRequest(handle);
        try {
            nn.closeFile(closeRequest);
        } catch (Exception e) {}
    }
    
    public void writeFile(String fileName, byte[] data) {
        Integer handle = openFileForWrite(fileName);
        byte[] assignBlockRequest = ProtoMessage.assignBlockRequest(handle);
        int dlength = data.length;
        for(int i=0; i<dlength; i+=CHUNK_SIZE) {
            byte[] chunk_data = Arrays.copyOfRange(data, i, Math.min(i+CHUNK_SIZE, dlength));
            Hdfs.BlockLocations blockLocations = null;
            try {
                byte[] response = nn.assignBlock(assignBlockRequest);
                blockLocations = Hdfs.AssignBlockResponse.parseFrom(response).getNewBlock();
            } catch (Exception e) {}
            // NOTE: Assuming Data Node lacation's port represents its id.
            IDataNode dn = dns.get(blockLocations.getLocations(0).getPort());
            byte[] writeBlockRequest = ProtoMessage.writeBlockRequest(chunk_data, blockLocations);
            try {
                dn.writeBlock(writeBlockRequest);
            } catch (Exception e) {}
        }
        closeFile(handle);
    }
    
    public void readFile(String fileName) {
        ByteString data = ByteString.EMPTY;
        List<Hdfs.BlockLocations> blockLocations = openFileForRead(fileName);
        for (Hdfs.BlockLocations block: blockLocations) {
            Random rand = new Random();
            Integer dataNodeInd = rand.nextInt(DN_COUNT);
            Hdfs.DataNodeLocation dnl = block.getLocations(dataNodeInd);
            Integer dataNodeId = dnl.getPort();
            log("Pulling data from DN " + dataNodeId.toString());
            try {
                byte[] request = ProtoMessage.readBlockRequest(block.getBlockNumber());
                byte[] response = dns.get(dataNodeId).readBlock(request);
                Hdfs.ReadBlockResponse readBlockResponse = Hdfs.ReadBlockResponse.parseFrom(response);
                data = data.concat(readBlockResponse.getData(0));
            } catch (Exception e) { log(e.toString()); }
        }
        System.out.println("File " + fileName + " has contents: " + data.toStringUtf8());
    }
    
    public void mainloop() {
        Scanner in = new Scanner(System.in);
        for(;;) {
            String line = in.nextLine();
            String[] ip = line.split(" ");
            if(ip[0].contentEquals("write"))
                writeFile(ip[1], ip[2].getBytes());
            else if(ip[0].contentEquals("read"))
                readFile(ip[1]);
        }
    }
}
