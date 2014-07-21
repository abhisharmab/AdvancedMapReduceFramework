/**
 * 
 */
package abhi.adfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Douglas Rew 
 * This is the interface for the NameNodeMaster
 *
 */
public interface NameNodeMaster extends Remote{
	 
	// This is used to register the DataNode to the Master
	public boolean registerToMaster(String dataNodename) throws RemoteException;
	
	// This is used to check the file existence of an input
	public boolean checkFileExistence(String fileName) throws RemoteException;
	
	// This is used to register the InputFileInfo to the Master
	public void registerFileInformation(InputFileInfo fileInfo) throws RemoteException;
	
	// This is used to get the InputFileInfo by the filename
	public InputFileInfo getInputFileInfo(String fileName) throws RemoteException;
	
	// This is used to remove the file in the DFS
	// This will trigger the all DataNodes that has a partitioned file.
	public void removeInputFileInfo(String fileName) throws RemoteException;
	
	// This will give a list of all of the registered DataNodes to the Master
	public List<String> getDataNodes() throws RemoteException;
	
	// This is used to check the liveness of the Master
	public boolean ping() throws RemoteException;

	// For debugging
	//public void print() throws RemoteException;

}
