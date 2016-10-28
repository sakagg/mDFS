/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;

import Proto.Hdfs;
import Proto.ProtoMessage;

/**
 *
 * @author saksham
 */
public class DataNodeLocation {
    String ip;
    Integer port;

    public DataNodeLocation(String i, Integer p) {
        ip = i;
        port = p;
    }
    
    public Hdfs.DataNodeLocation.Builder toProto() {
        return ProtoMessage.dataNodeLocation(ip, port);
    }
}