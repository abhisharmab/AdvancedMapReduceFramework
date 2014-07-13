/**
 * 
 */
package abhi.adfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Douglas Rew
 *
 */
public interface NameNodeMaster extends Remote{
	
	public boolean registerToMaster(String slave_name) throws RemoteException;
	//public boolean registerFileToMaster(String slave_name, String file_name) throws RemoteException;
	
	public boolean checkFileExistance(String fileName) throws RemoteException;
	public void registerFileInformation(InputFileInfo fileInfo) throws RemoteException;
	public InputFileInfo getInputFileInfo(String fileName) throws RemoteException;
	public void removeInputFileInfo(String fileName) throws RemoteException;
	public List<String> getDataNodes() throws RemoteException;
	public boolean ping() throws RemoteException;
	// For debugging
	public void print() throws RemoteException;

}
