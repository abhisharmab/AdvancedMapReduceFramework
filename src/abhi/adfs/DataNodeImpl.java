/**
 * 
 */
package abhi.adfs;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;

import abhi.mapreduce.SystemConstants;

/**
 * @author Douglas Rew 
 *
 */
public class DataNodeImpl extends UnicastRemoteObject implements DataNode{


	private List<String> fileList;
	private static String directory;
	private static String jar_directory;
	

	
	protected DataNodeImpl() throws RemoteException {
		super();
		directory = SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY);
		jar_directory = SystemConstants.getConfig(SystemConstants.JAR_DIRECTORY);
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
	

	public void checkJarDirectory(){
		File dir = new File(jar_directory);
		if(dir.exists()){
			System.out.println("Jar Directory for distributed file system exists.");
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
	
	public String getPathJar(String filename){
		String path = jar_directory + "/" + filename;
		System.out.println("Path for file   " + path);
		return path;
	}



	@Override
	public boolean remove(String filename) throws RemoteException {
		File file = new File(getPath(filename));
		if( file.exists()){
			if(file.delete()){
				System.out.println("File : " + filename + " has been deleted.");
				fileList.remove(filename);
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
				fileList.add(filename); 
			} else {
				System.out.println("File is already existing.");
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


	// This method is used to check the liveness of the DataNode.
	@Override
	public boolean ping() throws RemoteException {
		// TODO Auto-generated method stub
		return true;
	}


	@Override
	public List<String> getFileList() throws RemoteException {
		// TODO Auto-generated method stub
		return fileList;
	}


	@Override
	public boolean submitJar(String filename, String data)
			throws RemoteException {
		// TODO Auto-generated method stub
		
		BufferedWriter writer = null;
		File file = new File(getPathJar(filename));
		
		
		try {
			if( file.createNewFile()){
				
				writer = new BufferedWriter(new FileWriter(file));
				writer.write(data);
				System.out.println("Jar " + filename + " has been created.");
			} else {
				System.out.println("File is already existing, deleting the existing one and resaving it.");
				if(file.delete()){
					file.createNewFile();
					writer = new BufferedWriter(new FileWriter(file));
					writer.write(data);
					System.out.println("Jar " + filename + " has been created.");
				} else {
					System.out.println("Error while deleting the old JAR file.");
				}
				
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



}
