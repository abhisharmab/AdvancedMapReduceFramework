/**
 * 
 */
package abhi.adfs;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * @author Douglas Rew
 *
 */
public interface NameNodeMaster extends Remote{
	
	public boolean registerToMaster(String slave_name) throws RemoteException;
	public boolean registerFileToMaster(String slave_name, String file_name) throws RemoteException;
	// For debugging
	public void print() throws RemoteException;

}
