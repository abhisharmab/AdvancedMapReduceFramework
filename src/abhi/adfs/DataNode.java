/**
 * 
 */
package abhi.adfs;

import java.io.File;
import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Douglas Rew 
 * This is the interface for the DataNode. On each node there will be a DataNode living 
 * which will handle the creation / delete / retrieve of a file.
 */ 
public interface DataNode extends Remote {
	
	// This method is used to save files into the Distributed File System(DFS)
	public boolean submit(String filename, String data) throws RemoteException;
	
	// This method is used to save the JAR files into the DFS
	// We are using the byte[] to avoid corruption in the jar file. 
	public boolean submitJar(String filename, byte data[], int length) throws RemoteException;
	
	// This method is used to remove files from the DFS
	public boolean remove(String filename) throws RemoteException;
	
	// This method is used to retrieve data from the file
	public String retrieve(String filename) throws RemoteException;
	
	// This method is used to check whether the file exist in the DFS
	public boolean isExist(String filename) throws RemoteException;
	
	// This method is used to retrieve all the fileNames that the DataNode has registered. 
	public List<String> getFileList() throws RemoteException;
	
	// This method is used to register file name which were only created on the LocalDataNode
	public boolean registerFileName(String filename) throws RemoteException;
	
	// This method is used to determine whether the node is alive.
	public boolean ping() throws RemoteException;
	
	// This is for testing
	// public void print() throws RemoteException;
}
