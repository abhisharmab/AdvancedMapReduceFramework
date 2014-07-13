//package abhi.adfs;
//import java.io.IOException;
//import java.io.InputStreamReader;
//import java.lang.reflect.Constructor;
//import java.lang.reflect.InvocationTargetException;
//import java.util.Scanner;
//
//
//public class ProcessManager {
//
//	private static Master_Server master = null;
//	private static Slave_Server slave = null;
//	
//	
//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//		
//		
//		Scanner scanner = new Scanner(new InputStreamReader(System.in));
//		Boolean selection = Boolean.TRUE;
//		Boolean master_selected = Boolean.FALSE;
//		Boolean terminate = Boolean.FALSE;
//		
//		System.out.println("1: Startup a Master.");
//		System.out.println("2: Startup a Slave.");
//		System.out.println("3: Quit.");
//		
//		while(!terminate){
//			while(selection){
//				String input = scanner.nextLine();
//				if(input.equals("1")){
//					System.out.println("Enter Master Server's port Number.");
//					String port_num = scanner.nextLine().trim();
//					try {
//						master = new Master_Server(port_num);
//						master.start();
//						selection = Boolean.FALSE;
//						master_selected = Boolean.TRUE;
//						break;
//						
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						System.out.println("Press Enter.");
//					}
//
//				} else if( input.equals("2")){
//					
//					System.out.println("Enter Master's Address for Slave.");
//					String master_host = scanner.nextLine().trim();
//					System.out.println("Enter Master's Port Number for Slave.");
//					String master_port = scanner.nextLine().trim();
//					
//					try {
//						slave = new Slave_Server(master_host, master_port);
//						if( slave.isConnected()){
//							slave.run();
//							System.out.println("Slave has been terminated.");
//							System.out.println("Press Enter to see options.");
//						}
//					} catch (Exception e) {
//						// TODO Auto-generated catch block
//						System.out.println("Press Enter.    ");
//					}
//					
//					
//					
//					
//				} else if(input.equals("3")){
//					
//					System.out.println("Shutting Down");
//					selection = Boolean.FALSE;
//					terminate = Boolean.TRUE;
//					
//					
//				} else {
//					System.out.println("1: Startup a Master.");
//					System.out.println("2: Startup a Slave.");
//					System.out.println("3: Quit.");
//				}
//			}
//			
//			while(master_selected){
//
//				System.out.println();
//				System.out.println("Master Started.\n");
//				System.out.println("1: Start Process.");
//				System.out.println("2: List all Processes in Slaves");
//				System.out.println("3: Migrate a Process.");
//				System.out.println("4: Kill a Process in a Slave.");
//				System.out.println("5: Kill all Slaves.");
//				System.out.println("6: Quit.");
//				System.out.println();
//				
//				String input = scanner.nextLine().trim();
//				if(input.equals("1")){
//					
//					if(master.is_Slave_Connected()){
//						System.out.println("Listing slaves and processes.");
//						for(String slaves_status : master.broadcast_Message(Communication_Type.Status)){
//							System.out.println("--"+slaves_status);
//						}
//						
//						System.out.println("Select a Slave to start the process.");
//						System.out.println("Enter the Slave Id.");
//						
//						String slave_id = scanner.nextLine().trim();
//						try{
//							if( master.check_SlaveId(Integer.parseInt(slave_id))){
//								
//								System.out.println("Example usage: GrepProcess <queryString> <inputFile> <outputFile>.");
//								System.out.println("Example usage: WordCountProcess <inputFile> <outputFile>");
//								System.out.println("Example usage: UniqProcess <option> <inputFile> <outputFile>");
//								System.out.println("Example usage: <option> -u : unique -d duplicate");
//								
//								System.out.println("Enter in classname.");
//								String class_name = scanner.nextLine().trim();
//								System.out.println("Enter in arguments.");
//								String[] arguments = scanner.nextLine().trim().split(" ");
//								
//								master.start_Process_In_Slave(class_name, arguments, Integer.parseInt(slave_id));
//
//							} else {
//								System.out.println("Slave id does not match please check again.");
//							}
//						} catch (NumberFormatException e){
//							System.out.println("Input is not a number please retry.");
//						}
//						
//					} else {
//						System.out.println("There are no Slaves connected.");
//					}
//				
//					
//			
//					
//				} else if( input.equals("2")){
//
//					if(master.is_Slave_Connected()){
//						for(String slaves_status : master.broadcast_Message(Communication_Type.Status)){
//							System.out.println("--"+slaves_status);
//						}
//					} else {
//						System.out.println("There are no Slaves connected.");
//					}
//					
//				} else if(input.equals("3")){
//
//					
//					if(master.is_Slave_Connected()){
//						System.out.println("Listing slaves and processes.");
//						for(String slaves_status : master.broadcast_Message(Communication_Type.Status)){
//							System.out.println("--"+slaves_status);
//						}
//						
//						System.out.println("Select a Slave to start the process.");
//						System.out.println("Enter the Slave Id.");
//						
//						String slave_id = scanner.nextLine().trim();
//						try{
//							if( master.check_SlaveId(Integer.parseInt(slave_id))){
//								
//								System.out.println("Enter the PID.");
//								String process_id = scanner.nextLine().trim();
//								
//								System.out.println("Enter the Targeted Slave ID.");
//								String targeted_slave_id = scanner.nextLine().trim();
//								
//								master.migrate_Process_In_Slave(Integer.parseInt(slave_id), Integer.parseInt(process_id), Integer.parseInt(targeted_slave_id));
//
//							} else {
//								System.out.println("Slave id does not match please check again.");
//							}
//						} catch (NumberFormatException e){
//							System.out.println("Input is not a number please retry.");
//						}
//						
//						
//						
//					} else {
//						System.out.println("There are no Slaves connected.");
//					}
//
//				} else if(input.equals("4")){
//					if(master.is_Slave_Connected()){
//						for(String slaves_status : master.broadcast_Message(Communication_Type.Status)){
//							System.out.println("--"+slaves_status);
//						}
//						
//						System.out.println("Select a Slave to Stop the process.");
//						System.out.println("Enter the Slave Id.");
//						
//						String slave_id = scanner.nextLine().trim();
//						try{
//							if( master.check_SlaveId(Integer.parseInt(slave_id))){
//								
//								System.out.println("Enter the PID.");
//								String process_id = scanner.nextLine().trim();
//								
//								
//								master.suspend_Process_In_Slave(Integer.parseInt(slave_id), Integer.parseInt(process_id));
//
//							} else {
//								System.out.println("Slave id does not match please check again.");
//							}
//						} catch (NumberFormatException e){
//							System.out.println("Input is not a number please retry.");
//						}
//							
//						
//					} else {
//						System.out.println("There are no Slaves connected.");
//					}
//
//				} else if(input.equals("5")){
//					if(master.is_Slave_Connected()){
//						for(String slaves_status : master.broadcast_Message(Communication_Type.Kill_Slaves)){
//							System.out.println("--"+slaves_status);
//						}
//					} else {
//						System.out.println("There are no Slaves connected.");
//					}
//				} else if(input.equals("6")){
//					
//					if(master.isAllClosed()){
//						master.terminate();
//						master_selected = Boolean.FALSE;
//						selection = Boolean.TRUE;
//						System.out.println("Press Enter to see options.");
//					} else {
//						System.out.println("All slaves should be terminated before shutting down.");
//					}
//				} 
//				
//			}	
//		}
//		
//	}
//
//
//}
