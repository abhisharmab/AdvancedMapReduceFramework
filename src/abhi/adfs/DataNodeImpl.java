/**
 * 
 */
package abhi.adfs;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import abhi.mapreduce.SystemConstants;

/**
 * @author Douglas Rew 
 * This is the implementation of the DataNode
 */
public class DataNodeImpl extends UnicastRemoteObject implements DataNode{


	// This will be used to maintain the files
	private List<String> fileList;
	// This will be the directory for the DFS
	private static String directory;
	// This will be the directory for the jar files
	private static String jar_directory;
	

	
	protected DataNodeImpl() throws RemoteException {
		super();
		// Looking up locations.
		directory = SystemConstants.getConfig(SystemConstants.ADFS_DIRECTORY);
		jar_directory = SystemConstants.getConfig(SystemConstants.JAR_DIRECTORY);
		
		// Checking existence
		checkDirectory();
		checkJarDirectory();
		
		fileList = new ArrayList<String>();
		
	}
	
	// Checking existence
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
	

	// Checking existence
	public void checkJarDirectory(){
		File dir = new File(jar_directory);
		if(dir.exists()){
			System.out.println("Jar Directory for distributed file system exists.");
		} else {
			System.out.println("There is no existing directory.");
			System.out.println("Creating directory : " +jar_directory);
			dir.mkdir();
		}
	}
	
	// This will append the correct file path on the filename
	public String getPath(String filename){
		String path = directory + System.getProperty("file.separator") + filename;
		//System.out.println("Path for file   " + path);
		return path;
	}

	// This will append the correct file path on the filename
	public String getPathJar(String filename){
		String path = jar_directory + System.getProperty("file.separator") + filename;
		System.out.println("Path for file   " + path);
		return path;
	}



	// This will be used to remove the file in the DFS
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



//  This was for testing
//	@Override
//	public void print() throws RemoteException {
//		System.out.println("this is from the datanodeimpl");
//		
//	}


	// This is used to save data with the filename on the DataNode
	// After creating the file it will update the filelist On the DataNode for future use.
	// This is used for the input files.
	@Override
	public boolean submit(String filename, String data) throws RemoteException {
		
		BufferedWriter writer = null;
		File file = new File(getPath(filename));
		
		try {
			if( file.createNewFile()){
				
				writer = new BufferedWriter(new FileWriter(file));
				writer.write(data);
				System.out.println("File " + filename + " has been created.");
				// Adding the filename to the fileList
				fileList.add(filename); 
			} else {
				// We do not create the existing file again.
				System.out.println("File is already existing.");
			}
		} catch (IOException e) {
			System.out.println("There error writing the file : " + filename);
			e.printStackTrace();
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


	// This method is used to save the Jar file in the node
	// The jar files will not be tracked by the DataNode
	// Because it will be used only for the execution.
	@Override
	public boolean submitJar(String filename, byte data[], int length)
			throws RemoteException {
	
		// We delete the existing jar files before Saving it.
		File file = new File(getPathJar(filename));
		if( file.exists()){
			file.delete();
		}
		
		// Start the saving process for the JAR file
		BufferedOutputStream output;
		try {
			output = new
				 BufferedOutputStream(new FileOutputStream(getPathJar(filename)));
	        output.write(data,0,length);
	        output.flush();
	        output.close();
	        
	        
	    	//Extracting the Jar file for future 
	        JarExtraction ex = new JarExtraction(filename);
	        ex.extraction();
	        
	        
	        
	        return true;
		} catch (FileNotFoundException e2) {
			// TODO Auto-generated catch block
			System.out.println("Error in file, please retry.");
			return false;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Error in IO, please retry.");
			return false;
		} catch (Exception e){
			System.out.println("Error please retry.");
			e.printStackTrace();
			return false;
		}
		
	
		
		
	}


	// This method is used to retrieve the file in a string
	@Override
	public String retrieve(String filename) throws RemoteException {
		
		Scanner scan;
		try {
			File file = new File(getPath(filename));
			scan = new Scanner(file);
			StringBuilder data = new StringBuilder();
			while (scan.hasNextLine()){
				String temp = scan.nextLine();
				data.append("\n"+temp);
			}
			
			scan.close();
			return data.toString();
		} catch (FileNotFoundException e) {
			System.out.println("Cannot Retrieve filename : " + filename);
			System.out.println("Please Check Again.");
		}
		
		return null;
		
	}


	// This method is to register the fileName to the DataNode
	@Override
	public boolean registerFileName(String filename) throws RemoteException {
		if(fileList.contains(filename)){
			System.out.println("Cannot Register FileName : " + filename);
			System.out.println("Alreay exist.");
			return false;
		} else {
			fileList.add(filename);
			return true;
			
		}
		
	}



}
