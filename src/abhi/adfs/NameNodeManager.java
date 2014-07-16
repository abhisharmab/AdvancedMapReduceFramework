/**
 * 
 */
package abhi.adfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
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

import abhi.mapreduce.SystemConstants;

/**
 * @author dkrew
 *
 */
public class NameNodeManager {
	
	private static String portNumber = null;
	private static String ipAddress = null;
	private static NameNodeSlave slave;
	
	public static void main(String args[])
    {
		System.out.println(args.length);
		if ( !(1 <= args.length && args.length <= 2)) {
			System.err.println("Usage: NameNodeManager --dump <Input File Name>");
			System.err.println("Usage: NameNodeManager --jar <Jar File Name>");
			System.err.println("Usage: NameNodeManager --remove <File Name>");
			System.err.println("Usage: NameNodeManager --cat ");
			System.err.println("Usage: NameNodeManager --help");
			return;
		}
		
		
		String option = (String) args[0];
		if(option.equals("--jar") || option.equals("--dump") || option.equals("--remove") || option.equals("--cat")){
			try
	        {
	    	   if(System.getSecurityManager() == null){
	    		   System.setSecurityManager(new RMISecurityManager());   
	    	   }
	        	
	    	   
//		   		portNumber = SystemConstants.getConfig(SystemConstants.REGISTRY_PORT);
//		        ipAddress = SystemConstants.getConfig(SystemConstants.REGISTRY_HOST);
	        
	        
		        String identifer = InetAddress.getLocalHost().getCanonicalHostName();
		        String nameNodeSlave = "NameNodeSlave_" + identifer;
	        //	String lookup_name = "rmi://" +ipAddress + ":"+ portNumber+ "/" + nameNodeSlave;
	        
//	        	System.out.println(lookup_name);
	        	slave =  (NameNodeSlave) Naming.lookup(nameNodeSlave);
	    		System.out.println("NameNodeSlave has been looked up.");
	    	} catch (Exception e){
	    		e.printStackTrace();
	    		System.out.println("Manager: Exception thrown looking up " + "NameNodeSlave");
	    		System.out.println("Please check all systems and try again.");
	    		
	    		
	    	}
			
			if( option.equals("--dump")){
				String input_filename = (String)args[1];
				try { 
					if(slave.dump(input_filename)){
						System.out.println("File : " + input_filename + " has been distributed.");
					} else {
						System.out.println("Error has been occured while distributing the file.");
					}
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if( option.equals("--jar")){
					String input_filename = (String)args[1];
					try { 
						if(slave.dumpJar(input_filename)){
							System.out.println("Jar : " + input_filename + " has been distributed.");
						} else {
							System.out.println("Error has been occured while distributing the file.");
						}
					} catch (RemoteException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
			} else if (option.equals("--remove")){
				String remove_filename = (String) args[1];
				try {
					if(slave.remove(remove_filename)){
						System.out.println("File : " + remove_filename + " has been removed.");
					} else {
						System.out.println("Error has been occured while removing the file.");
					}
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else if ( option.equals("--cat")){
				List<String> files = null;
				try {
					files = slave.getDataNodeFiles();
					System.out.println("DataNode contains files...");
					for(String name : files){
						System.out.println("---" + name);
					}
				} catch (RemoteException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
				
		} else {
			System.err.println("Usage: NameNodeManager --dump <Input File Name>");
			System.err.println("Usage: NameNodeManager --jar <Jar File Name>");
			System.err.println("Usage: NameNodeManager --remove <File Name>");
			System.err.println("Usage: NameNodeManager --cat ");
			System.err.println("Usage: NameNodeManager --help");
		}


 
    }


}
 