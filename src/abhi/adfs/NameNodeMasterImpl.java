/**
 * 
 */
package abhi.adfs;

import java.net.MalformedURLException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * @author Douglas Rew
 *
 */
public class NameNodeMasterImpl extends UnicastRemoteObject implements NameNodeMaster{

	/**
	 * 
	 */
	private static final long serialVersionUID = 2917579042804203973L;
	private static String master_Name;
	
	// This will keep track of the file information.
	private static List<InputFileInfo> list_fileInfo;
	private static HashMap<String, DataNode> list_dataNode;
	
	public NameNodeMasterImpl() throws RemoteException {
		super();
		list_dataNode = new HashMap<String, DataNode>();
		setList_fileInfo(new ArrayList<InputFileInfo>());
		// TODO Auto-generated constructor stub
	}
	
	public boolean registerToMaster(String slave_name) 
			throws RemoteException {

		registerDataNode(slave_name);
		return true;
		

	}
//
//	@Override
//	public boolean registerFileToMaster(String slave_name, String file_name)
//			throws RemoteException {
//		// TODO Auto-generated method stub
//		if(list_distributed_files.containsKey(slave_name)){
//			if(list_distributed_files.get(slave_name).contains(file_name)){
//				System.out.println("This file is already registered Filename : " + file_name + " in slave : " + slave_name);
//				// Might need to throw exception
//				return false;
//			} else {
//				list_distributed_files.get(slave_name).add(file_name);
//				return true;
//			}
//			
//		} else {
//			return false;	
//		}
//		
//	}
	
	public static void main(String args[])
    {
		if (args.length != 1) {
			System.err.println("Usage: NameNodeMaster <RMI_port> ");
			return;
		}
		
		
        try
        {
        	master_Name = "NameNodeMaster";
            System.out.println("Registering NameNodeMaster as : NameNodeMaster");
            NameNodeMasterImpl master = new NameNodeMasterImpl();
            Naming.rebind(master_Name, master);
            System.out.println("NameNodeMaster: Ready...");
        }
        catch (Exception e)
        {
            System.out.println("Server: Failed to register NameNodeMaster: " + e);
        }
    }

	@Override
	public void print() throws RemoteException {
		// TODO Auto-generated method stub
		System.out.println("This is me!!!!!");
	}

	void registerDataNode(String dataNodeName){
		
        try {
			DataNode dataNode = (DataNode) Naming.lookup(dataNodeName);
			System.out.println("Looked up datanNode : " + dataNodeName);
			
			list_dataNode.put(dataNodeName, dataNode);
			
			System.out.println(list_dataNode);
			
			
			
			dataNode.print();
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (RemoteException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NotBoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        
	}

	@Override
	public void registerFileInformation(InputFileInfo fileInfo)
			throws RemoteException {
		
		
		if(list_fileInfo.isEmpty()){
			list_fileInfo.add(fileInfo);	
		} else {
			if(!checkFileExistance(fileInfo.getFileName())){
				list_fileInfo.add(fileInfo); 
			}
		}
		

		
		
		
		System.out.println("registerFileInformation    : " + fileInfo.getFileName());

		
		for(InputFileInfo info : list_fileInfo){
			System.out.println("-------"+info.getFileName());
		}
		
		
	}

	public static List<InputFileInfo> getList_fileInfo() {
		return list_fileInfo;
	}

	public static void setList_fileInfo(List<InputFileInfo> list_fileInfo) {
		NameNodeMasterImpl.list_fileInfo = list_fileInfo;
	}

	@Override
	public List<String> getDataNodes() throws RemoteException {
		// TODO Auto-generated method stub
		
		List<String> live_DataNode = new ArrayList<String>();
		Iterator iter = list_dataNode.entrySet().iterator();
		while(iter.hasNext()){
			Entry<String, DataNode> dn = (Entry<String, DataNode>) iter.next();
		
			try{
				if(dn.getValue().ping()){
					live_DataNode.add(dn.getKey());
				} 
			} catch(RemoteException e){
				System.out.println("There are dead dataNode this should be addressed");
				iter.remove();
				
			}
			
			
		}
		
		
		System.out.println(live_DataNode.toString());
		return live_DataNode;
	}

	@Override
	public boolean checkFileExistance(String fileName) throws RemoteException {
		// TODO Auto-generated method stub
		
		System.out.println("checkFileExistance   " + fileName);
		
		for(InputFileInfo info : list_fileInfo){
			System.out.println("Info name  " + info.getFileName());
		}
		
		
		for(InputFileInfo info : list_fileInfo){
			System.out.println("Info name  " + info.getFileName());
			if(info.getFileName().equals(fileName)){
				return true;
			}
				
		}
		return false;
		
	}

	@Override
	public InputFileInfo getInputFileInfo(String fileName)
			throws RemoteException {
		for(InputFileInfo info : list_fileInfo){
			if( info.getFileName().equals(fileName)){
				return info;
			}
		}
		return null;
	}

	@Override
	public void removeInputFileInfo(String fileName)
			throws RemoteException {
		Iterator<InputFileInfo> iter = list_fileInfo.iterator();
		while(iter.hasNext()){
			InputFileInfo info = iter.next();
			if(info.getFileName().equals(fileName)){
				cleanUpDataNodes(info);
				iter.remove();
			}
		}
		
	}
	
	public void cleanUpDataNodes(InputFileInfo info){
		for(Entry<String, List<String>> entry : info.getPartitions().entrySet()){
			DataNode dn = list_dataNode.get(entry.getKey());
			try {
				if(dn.ping()){
					System.out.println("Cleaning up all partitioned files.");
					for(String filename : entry.getValue()){
						if(dn.remove(filename)){
							System.out.println("File : " + filename + " has been removed.");
							
						} else {
							System.out.println("Error has been occured while removing file : " + filename);
						}
						
					}
					
				} else {
					System.out.println("DataNode : " + entry.getKey() + " is dead.");
					list_dataNode.remove(entry.getKey());
					
					// Should add in code that will go through the inputfileinfo and clean up and
					// and save partitioned files
					
				}
			} catch (RemoteException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
	}

	@Override
	public boolean ping() throws RemoteException {
		// TODO Auto-generated method stub
		return true;
	}
}
