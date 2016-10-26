/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DataNode;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 *
 * @author saksham
 */
public interface IDataNode extends Remote {

	/* ReadBlockResponse readBlock(ReadBlockRequest)) */
	/* Method to read data from any block given block-number */
	byte[] readBlock(byte[] inp) throws RemoteException;
	
	/* WriteBlockResponse writeBlock(WriteBlockRequest) */
	/* Method to write data to a specific block */
	byte[] writeBlock(byte[] inp) throws RemoteException;	
}

