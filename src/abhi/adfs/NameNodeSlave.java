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
public interface NameNodeSlave extends Remote{
	 

	public boolean dump(String fileName)  throws RemoteException;
	public boolean dumpJar(String fileName)  throws RemoteException;
	public boolean ping() throws RemoteException;
	public boolean remove(String fileName) throws RemoteException;
	public List<String> getDataNodeFiles() throws RemoteException;
	public boolean registerToDataNode(String fileName) throws RemoteException;
	// For debugging
	public void print() throws RemoteException;

}
