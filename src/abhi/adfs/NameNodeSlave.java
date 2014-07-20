/**
 * 
 */
package abhi.adfs;

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;

/**
 * @author Douglas Rew 
 * This is the interface for the NameNodeSlave
 */
public interface NameNodeSlave extends Remote{
	 

	// This is used to save the input file into the DFS
	public boolean dump(String fileName)  throws RemoteException;
	
	// This is used to save the Jar file into the DFS
	public boolean dumpJar(String fileName)  throws RemoteException;
	
	// This is used to check the liveness
	public boolean ping() throws RemoteException;
	
	// This is used to remove files from the DFS
	public boolean remove(String fileName) throws RemoteException;
	
	// This is used to get the information of the Files in the DataNode
	public List<String> getDataNodeFiles() throws RemoteException;
	
	// This is used to register file into the Local DataNode
	// This will be called by the Mapper
	public boolean registerToLocalDataNode(String fileName) throws RemoteException;
	
	// This is used to retrieve file from the Local Data Node
	// This will be called by the Reducer
	public String retrieveFromLocalDataNode(String fileName) throws RemoteException;
	
	// This is used to check the existences of a file in the DFS
	public boolean checkFileExistences(String fileName) throws RemoteException;
	
	// For debugging
//	public void print() throws RemoteException;

}
