/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package NameNode;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;

/**
 *
 * @author saksham
 */
public class NameNode implements INameNode {
    
    NameNode() throws RemoteException {
        super();
    }
    
    public static void main(String args[]) {
        try {
            LocateRegistry.createRegistry(1099);
        } catch (RemoteException e) { }
    }
    
    @Override
    public byte[] openFile(byte[] inp) throws RemoteException {
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
}
