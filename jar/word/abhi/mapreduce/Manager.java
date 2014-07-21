/**
 * 
 */
package abhi.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.rmi.AlreadyBoundException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RMISecurityManager;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Scanner;

import abhi.adfs.JarExtraction;
import abhi.adfs.NameNodeSlave;
import abhi.adfs.NameNodeSlaveImpl;

/**
 * @author dkrew
 * This will be used to interact with the NameNodeSlave by the user.
 * 			Usage: Manager --dump <Input File Name>
			Usage: Manager --jar <Jar File Name>
			Usage: Manager --remove <File Name>
			Usage: Manager --cat 
 * 
 */
public class Manager {
	
	private static String portNumber = null;
	private static String ipAddress = null;
	private static NameNodeSlave slave;
	
	public static void main(String args[])
    {
		boolean error = true;
		if ( args.length > 0) {
			String option = (String) args[0];
			
			if( option.equals("--dump")){
				if((args.length == 2)){
					error = false;
				}
			} else if (option.equals("--remove")){
				if((args.length == 2)){
					error = false;
				}
			} else if  (option.equals("--ls") || option.equals("--help")){
				if((args.length == 1)){
					error = false;
				}
			} else if (option.equals("--jar")){
				if((args.length == 5)){
					error = false;
				}
			}
		} 
		
		
		
		// Print the help information if there are errors in args
		if( error){
			System.err.println("Usage: Manager --dump <Input File Name>");
			System.err.println("Usage: Manager --remove <File Name>");
			System.err.println("Usage: Manager --ls ");
			System.err.println("Usage: Manager --jar <JarFileName> <Execution Class Name> <InputFileName> <OutputLocation>");
			System.err.println("Usage: Manager --help");
			return;
		}
		
		
		
		
		
		String option = (String) args[0];
		if( option.equals("--dump") || option.equals("--remove") || option.equals("--ls") || option.equals("--jar")){
			try
	        {
	    	   if(System.getSecurityManager() == null){
	    		   System.setSecurityManager(new RMISecurityManager());   
	    	   }
	        
	  
	        
		        String identifer = InetAddress.getLocalHost().getHostAddress();
		        String slave_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
		        String lookupName = slave_Name +"_" + identifer;
		        
		        System.out.println("lookupName   " + lookupName);
	        
	        	slave =  (NameNodeSlave) Naming.lookup(lookupName);
	    	} catch (Exception e){
	    		
	    		System.out.println("Error while accessing the Distributed File System.");
	    		System.out.println("Check on the DFS and try again.");
	    		return;
	    	}
			
			// This option will take the input file and try to partition it and save it
			// to different nodes in the DFS
			if( option.equals("--dump")){
				String input_filename = (String)args[1];
				try { 
					if(slave.dump(input_filename)){
						System.out.println("File : " + input_filename + " has been distributed.");
					} else {
						System.out.println("Error has been occured while distributing the file.");
					}
				} catch (RemoteException e) {
					System.out.println("Error in uploading the file " + input_filename);
					System.out.println("Please try again");
				}
				
		
			// This option is used to remove files from the DFS
			// The filename will be the original filename
			} else if (option.equals("--remove")){
				String remove_filename = (String) args[1];
				try {
					if(slave.remove(remove_filename)){
						System.out.println("File : " + remove_filename + " has been removed.");
					} else {
						System.out.println("Error has been occured while removing the file.");
					}
				} catch (RemoteException e) {
					System.out.println("Error in removing the file " + remove_filename);
					System.out.println("Please try again");
				}
				
				
				
			// This option is used to print out all the file in the local DataNode
			} else if ( option.equals("--ls")){
				List<String> files = null;
				try {
					files = slave.getDataNodeFiles();
					System.out.println("DataNode contains files...");
					for(String name : files){
						System.out.println("---" + name);
					}
				} catch (RemoteException e) {
					System.out.println("Error in geting local FileNames");
					System.out.println("Please try again");
				}
				
				
				
				
			// This option will send the Jar file to all Nodes 
			// and start the specified class with the input file and the output location.
			// System.err.println("Usage: Manager --jar <JarFileName> <Execution Class Name> <InputFileName> <OutputLocation>");
			} else if( option.equals("--jar")){
					String jarfileName = (String)args[1];
					String executionName = (String) args[2];
					String inputFileName = (String) args[3];
					String outputLocation = (String) args[4];
					try { 
						// Check on the output directory
						File outPutDictory = new File(outputLocation);
						if (outPutDictory.exists())						{
							System.out.println("Output Directory "+ outputLocation+ " Already Exists. Cannot Run Job");
							System.out.println("Check the output directory and try again.");
						} else {
							// Distributing the jar File
							if(slave.dumpJar(jarfileName)){
								
								// Extracting the jar file
								JarExtraction jar = new JarExtraction(jarfileName);
								if(jar.extraction()){
										
									// Distributing and Partitioning the input file
									if(slave.dump(inputFileName)){
										System.out.println("Paritioned the File.");
										System.out.println("Starting the execution.");
										
										String classPath = jar.getDictory();
										executeFile(executionName, inputFileName, outputLocation, classPath);
									} else {
										System.out.println("Error in distributing the input file.");
										System.out.println("Check the distribute file system and try again.");
									}
									
								} 
							} else {
								System.out.println("Error has been occured while distributing the Jarfile.");
							}
						}

						
					} catch (RemoteException e) {
					
						System.out.println("Error while accessing the DFS, check and please try again");
					}
			}
			
				
		} else {
			System.err.println("Usage: Manager --dump <Input File Name>");
			System.err.println("Usage: Manager --remove <File Name>");
			System.err.println("Usage: Manager --cat ");
			System.err.println("Usage: Manager --jar <JarFileName> <Execution Class Name> <InputFileName> <OutputLocation>");
			System.err.println("Usage: Manager --help");
		}


 
    }
	
	
	private static void executeFile(String executeName, String inputFile, String outputPath, String classPath){

		String separator = System.getProperty("file.separator");
		String[] args = new String[6];
		
		args[0] = "java";
		args[1] = "-cp";
		args[2] = classPath+";."+separator;
		args[3] = executeName;
		args[4] = inputFile;
		args[5] = outputPath;
		
		 
		ProcessBuilder p = new ProcessBuilder().command(args);
		//ProcessBuilder p = new ProcessBuilder().command(new String[] {"java", "-cp",  "./*", className});
		Process process;
		try {
			process = p.start();
			InputStream is = process.getInputStream();
		    InputStreamReader isr = new InputStreamReader(is);
		    BufferedReader br = new BufferedReader(isr);
		    String line;
		    while ((line = br.readLine()) != null)
		    {
		        System.out.println(line);
		    }
		    
		    try {
				System.exit(process.waitFor());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		    
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
	
		
	}


}
 