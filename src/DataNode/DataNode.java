/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataNode;

import Proto.Hdfs;
import Proto.ProtoMessage;
import java.nio.charset.StandardCharsets;
import java.rmi.Naming;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;

/**
 *
 * @author saksham
 */
public class DataNode extends UnicastRemoteObject implements IDataNode {
    private static final String DN_PREFIX = "DataNode";
    private static Integer myId = -1;
    private static String myName = "";
    
    DataNode() throws RemoteException {
        super();
    }
    
    public static void log(String s) {
        String op = String.valueOf(System.currentTimeMillis()) + " ";
        op += "[DataNode" + myId.toString() + "] ";
        op += ": ";
        System.out.println(op + s);
    }
    
    public static void main (String args[]) {
        myId = Integer.parseInt(args[1]);
        try {
            DataNode dn = new DataNode();
            Naming.rebind("rmi://localhost/" + DN_PREFIX + myId.toString(), dn);
            log("Bound to RMI");
        } catch (Exception e) { log(e.toString()); }
        for(;;) {}
    }
    
    @Override
        public byte[] readBlock(byte[] inp) throws RemoteException {
            return null;
        }
	
    @Override
        public byte[] writeBlock(byte[] inp) throws RemoteException {
            Hdfs.WriteBlockRequest writeBlockRequest = null;
            try {
                writeBlockRequest = Hdfs.WriteBlockRequest.parseFrom(inp);
            } catch (Exception e) { log(e.toString()); }
            byte[] data = writeBlockRequest.getData(0).toByteArray();
            log(new String(data, StandardCharsets.UTF_8));
            return ProtoMessage.writeBlockResponse(1);
        }
}
