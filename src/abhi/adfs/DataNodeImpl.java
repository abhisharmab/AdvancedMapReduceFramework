/**
 * 
 */
package abhi.adfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.Naming;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

/**
 * @author Douglas Rew
 *
 */
public class DataNodeImpl extends UnicastRemoteObject implements DataNode{


	private List<String> fileList;
	private static String directory = "adsf_files";
	
	protected DataNodeImpl() throws RemoteException {
		super();
		checkDirectory();
		fileList = new ArrayList<String>();
		
	}
	

	public void checkDirectory(){
		File dir = new File(directory);
		if(dir.exists()){
			System.out.println("Directory for distributed file system exists.");
		} else {
			System.out.println("There is no existing directory.");
			System.out.println("Creating directory : " +directory);
			dir.mkdir();
		}
	}
	
	public String getPath(String filename){
		String path = directory + "/" + filename;
		System.out.println("Path for file   " + path);
		return path;
	}
	




	@Override
	public boolean remove(String filename) throws RemoteException {
		File file = new File(getPath(filename));
		if( file.exists()){
			if(file.delete()){
				System.out.println("File : " + filename + " has been deleted.");
				return true;
			} else {
				System.out.println("Error in deleting the file : " + filename);
				return false;
			}
		} else {
			return false;	
		}
		
	}




	@Override
	public void print() throws RemoteException {
		System.out.println("this is from the datanodeimpl");
		
	}


	@Override
	public boolean submit(String filename, String data) throws RemoteException {
		// TODO Auto-generated method stub
		
		BufferedWriter writer = null;
		File file = new File(getPath(filename));
		
		
		try {
			if( file.createNewFile()){
				
				writer = new BufferedWriter(new FileWriter(file));
				writer.write(data);
				System.out.println("File " + filename + " has been created.");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e.toString());
			return false;
		} finally {
		
			try {
				if(writer !=null){
					writer.close();
					return true;
				}
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				System.out.println(e.toString());
				return false;
			}
			
		}
		return false;
	}


	// This will be a check method for the exist partitioned file name.
	@Override
	public boolean isExist(String filename) throws RemoteException {
	
		File file = new File(getPath(filename));
		if( file.exists() && file.isFile()){
			
			return true;
	
		} else {
			return false;	
		}
	}



}
