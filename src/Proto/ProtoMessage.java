/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Proto;

/**
 *
 * @author saksham
 */
public class ProtoMessage {
    public static byte[] openFileRequest(String filename, Boolean forRead) {
        Hdfs.OpenFileRequest.Builder builder = Hdfs.OpenFileRequest.newBuilder();
        builder.setFileName(filename);
        builder.setForRead(forRead);
        return builder.build().toByteArray();
    }
}
