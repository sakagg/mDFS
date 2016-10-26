/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author saksham
 */
public interface INameNode extends Remote {

	/* OpenFileResponse openFile(OpenFileRequest) */
	/* Method to open a file given file name with read-write flag*/
	byte[] openFile(byte[] inp) throws RemoteException;
	
	/* CloseFileResponse closeFile(CloseFileRequest) */
	byte[] closeFile(byte[] inp ) throws RemoteException;
	
	/* BlockLocationResponse getBlockLocations(BlockLocationRequest) */
	/* Method to get block locations given an array of block numbers */
	byte[] getBlockLocations(byte[] inp ) throws RemoteException;
	
	/* AssignBlockResponse assignBlock(AssignBlockRequest) */
	/* Method to assign a block which will return the replicated block locations */
	byte[] assignBlock(byte[] inp ) throws RemoteException;
	
	/* ListFilesResponse list(ListFilesRequest) */
	/* List the file names (no directories needed for current implementation */
	byte[] list(byte[] inp ) throws RemoteException;
	
	/*
		Datanode <-> Namenode interaction methods
	*/
	
	/* BlockReportResponse blockReport(BlockReportRequest) */
	/* Get the status for blocks */
	byte[] blockReport(byte[] inp ) throws RemoteException;
	
	/* HeartBeatResponse heartBeat(HeartBeatRequest) */
//	/* Heartbeat messages between NameNode and DataNode */
	byte[] heartBeat(byte[] inp ) throws RemoteException;
}
