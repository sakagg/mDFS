/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Client;

import NameNode.INameNode;
import DataNode.IDataNode;
import NameNode.NameNode;
import Proto.Hdfs;
import Proto.ProtoMessage;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author saksham
 */
public class Client {
    private static final String NN_NAME = "NameNode";
    private static final String DN_PREFIX = "DataNode";
    private static final Integer CHUNK_SIZE = 10;
    private static final Properties props = new Properties();
    
    private INameNode nn = null;
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[Client] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    public static void main(String args[]) {
        try {
            props.load(new BufferedReader(new FileReader("config.properties")));
        } catch (IOException ex) {
            Logger.getLogger(NameNode.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        Client client = new Client();
        client.findnn();
        client.mainloop();
    }
    
    public void findnn() {
        while(nn == null)
        {
            try {
                nn = (INameNode) Naming.lookup("rmi://" + props.getProperty("rmi.namenode.ip")
                        + ":" + props.getProperty("rmi.namenode.port") + "/" + NN_NAME);
                log("Found Name Node");
            } catch (NotBoundException | MalformedURLException | RemoteException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
            if (nn == null)
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {}
        }
    }
    
    public void listFiles() {
        byte[] inp = "*".getBytes();
        try {
            byte[] res = nn.list(inp);
            Hdfs.ListFilesResponse listFilesResponse = Hdfs.ListFilesResponse.parseFrom(res);
            if(listFilesResponse.getStatus() == 1) {
                listFilesResponse.getFileNamesList().stream().forEach((fileName) -> {
                    System.out.println(fileName);
                });
                System.out.println("");
            } else {
                System.err.println("Some Error Occured During Listing File");
            }
        } catch (RemoteException | InvalidProtocolBufferException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
        
    }
    
    public Integer openFileForWrite(String filename) {
        byte[] openRequest = ProtoMessage.openFileRequest(filename, Boolean.FALSE);
        Integer handle = -1;
        try {
            byte[] openResponse = nn.openFile(openRequest);
            handle = Hdfs.OpenFileResponse.parseFrom(openResponse).getHandle();
        } catch (InvalidProtocolBufferException | RemoteException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
        return handle;
    }
    
    public List<Hdfs.BlockLocations> openFileForRead(String fileName) {
        byte[] openRequest = ProtoMessage.openFileRequest(fileName, Boolean.TRUE);
        List<Hdfs.BlockLocations> blockLocations = null;
        try {
            byte[] res = nn.openFile(openRequest);
            Hdfs.OpenFileResponse openResponse = Hdfs.OpenFileResponse.parseFrom(res);
            if(openResponse.getStatus() == 1) {
                byte[] blockLocationRequest = ProtoMessage.blockLocationRequest(openResponse.getBlockNumsList());
                byte[] blockLocationResponse = nn.getBlockLocations(blockLocationRequest);
                Hdfs.BlockLocationResponse response = Hdfs.BlockLocationResponse.parseFrom(blockLocationResponse);
                blockLocations = response.getBlockLocationsList();
            }
        } catch (RemoteException | InvalidProtocolBufferException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
        return blockLocations;
    }
    
    public void closeFile(Integer handle) {
        byte[] closeRequest = ProtoMessage.closeFileRequest(handle);
        try {
            nn.closeFile(closeRequest);
        } catch (Exception e) {}
    }
    
    IDataNode getDataNode(String ip, Integer port) {
        IDataNode  dn = null;
        try {
            dn = (IDataNode) Naming.lookup("rmi://" + ip
                    + ":" + port
                    + "/" + DN_PREFIX);
        } catch (NotBoundException | MalformedURLException | RemoteException ex) {
            Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
        }
        return dn;
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
            } catch (RemoteException | InvalidProtocolBufferException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            IDataNode dn = getDataNode(blockLocations.getLocations(0).getIp(), blockLocations.getLocations(0).getPort());
            byte[] writeBlockRequest = ProtoMessage.writeBlockRequest(chunk_data, blockLocations);
            try {
                dn.writeBlock(writeBlockRequest);
            } catch (RemoteException ex) {
                Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        closeFile(handle);
    }
    
    public void readFile(String fileName) {
        ByteString data = ByteString.EMPTY;
        List<Hdfs.BlockLocations> blockLocations = openFileForRead(fileName);
        if(blockLocations != null) {
            for (Hdfs.BlockLocations block: blockLocations) {
                Random rand = new Random();
                Integer dataNodeInd = rand.nextInt(block.getLocationsCount());
                Hdfs.DataNodeLocation dnl = block.getLocations(dataNodeInd);
                log("Pulling data from DN " + dnl.getIp() + ":" + dnl.getPort());
                try {
                    byte[] request = ProtoMessage.readBlockRequest(block.getBlockNumber());
                    byte[] response = getDataNode(dnl.getIp(), dnl.getPort()).readBlock(request);
                    Hdfs.ReadBlockResponse readBlockResponse = Hdfs.ReadBlockResponse.parseFrom(response);
                    data = data.concat(readBlockResponse.getData(0));
                } catch (RemoteException | InvalidProtocolBufferException ex) {
                    Logger.getLogger(Client.class.getName()).log(Level.SEVERE, null, ex);
                }
            }
            System.out.println("File " + fileName + " has contents: " + data.toStringUtf8());
        }
        else {
            System.out.println("File : " + fileName + " does not exist.");
        }
    }
    
    public void mainloop() {
        Scanner in = new Scanner(System.in);
        for(;;) {
            String line = in.nextLine();
            String[] ip = line.split(" ");
            switch (ip[0]) {
                case "write":
                    writeFile(ip[1], ip[2].getBytes());
                    break;
                case "read":
                    readFile(ip[1]);
                    break;
                case "list":
                    listFiles();
                    break;
                default:
                    break;
            }
        }
    }
}
