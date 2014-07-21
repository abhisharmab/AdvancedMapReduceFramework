package abhi.adfs;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.rmi.Naming;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.util.Enumeration;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import abhi.mapreduce.InputFormat;
import abhi.mapreduce.KeyValueConstruct;
import abhi.mapreduce.SystemConstants;

public class Testing_Spliter {

	public static void main(String[] args) throws IOException {
		
		
		try {
//			File file = new File("WordCountInputFormat.class");
//			if(file.exists()){
				InputFormat inputFormat = (InputFormat) Class.forName("abhi.wordcount.WordCountInputFormat")
						.getConstructor(String.class)
						.newInstance("sample.txt");
				
				int count = 0;
				while(inputFormat.hasNext()){
					KeyValueConstruct kv = inputFormat.next();
					System.out.println(count);
					System.out.println(kv.key);
					System.out.println(kv.value);
					count++;
				}
//			} else {
//				System.out.println("no");
//			}
			
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (NoSuchMethodException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
//		JarExtraction ex = new JarExtraction("HelloWorld.jar");
//		ex.extraction();
		
//		 java.util.jar.JarFile jarfile = new java.util.jar.JarFile(new java.io.File("Hello111.jar"));
//		    java.util.Enumeration<java.util.jar.JarEntry> enu= jarfile.entries();
//		    while(enu.hasMoreElements())
//		    {
//		        String destdir = "jar/";     //abc is my destination directory
//		        java.util.jar.JarEntry je = enu.nextElement();
////
//		        System.out.println(je.getName());
//
//		        java.io.File fl = new java.io.File(destdir, je.getName());
//		        if(!fl.exists())
//		        {
//		            fl.getParentFile().mkdirs();
//		            fl = new java.io.File(destdir, je.getName());
//		        }
//		        if(je.isDirectory())
//		        {
//		            continue;
//		        }
//		        java.io.InputStream is = jarfile.getInputStream(je);
//		        java.io.FileOutputStream fo = new java.io.FileOutputStream(fl);
//		        while(is.available()>0)
//		        {
//		            fo.write(is.read());
//		        }
//		        fo.close();
//		        is.close();
//		    }
//
//		  
//		    System.out.println("wqfqwf");
//        String identifer = InetAddress.getLocalHost().getHostName();
//        
//        String slave_Name = SystemConstants.getConfig(SystemConstants.NAMENODE_SLAVE_SERVICE);
//        String lookupName = slave_Name +"_" + identifer;
//        
//        System.out.println("lookupName   " + lookupName);
    
//        try {
//			NameNodeSlave slave =  (NameNodeSlave) Naming.lookup(lookupName);
//			
//		} catch (NotBoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
    	
//		
//		Scanner scan = new Scanner(System.in);
//		String input = scan.nextLine();
//		runClassName(input);
//		ProcessBuilder p = new ProcessBuilder().command(new String[] {"java", "-cp",  "./jar", "abhi.adfs.HelloWorld"});
//		
//		Process process = p.start();
//		
//		InputStream is = process.getInputStream();
//		
//	    InputStreamReader isr = new InputStreamReader(is);
//	    BufferedReader br = new BufferedReader(isr);
//	    String line;
//	    while ((line = br.readLine()) != null)
//	    {
//	        System.out.println(line);
//	    }
//	    
//	    try {
//	    	System.out.println("watiing");
//			System.exit(process.waitFor());
//			System.out.println("watiing");
//		} catch (InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
//		Process proc = Runtime.getRuntime().exec("java -cp . abhi.adsf.HellWorld");
//		try {
//			JarFile jarfile = new JarFile(new File("HelloWorld.jar")); //jar file path(here sqljdbc4.jar)
//		    Enumeration<java.util.jar.JarEntry> enu= jarfile.entries();
//		    while(enu.hasMoreElements())
//		    {
//		        String destdir = "jar/";     
//		        java.util.jar.JarEntry je = enu.nextElement();
//
//		        System.out.println(je.getName());
//
//		        File fl = new File(destdir, je.getName());
//		        if(!fl.exists())
//		        {
//		            fl.getParentFile().mkdirs();
//		            fl = new java.io.File(destdir, je.getName());
//		        }
//		        if(je.isDirectory())
//		        {
//		            continue;
//		        }
//		        InputStream is = jarfile.getInputStream(je);
//		        FileOutputStream fo = new FileOutputStream(fl);
//		        while(is.available()>0)
//		        {
//		            fo.write(is.read());
//		        }
//		        fo.close();
//		        is.close();
//		    }
//
//
//			
//			
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
		
		
//		File dir = new File(directory);
//		if(dir.exists()){
//			System.out.println("Directory for distributed file system exists.");
//		} else {
//			System.out.println("There is no existing directory.");
//			System.out.println("Creating directory : " +directory);
//			dir.mkdir();
//		}
		
//		 Scanner s = new Scanner("hello\t world \n hello\t world");
//		 s.useDelimiter("\\t|\\n");
//		 while(s.hasNext()){
//		          System.out.println(s.next().trim());
//
//		 }
		 
		 
//		String temp;
//		try {
//			temp = InetAddress.getLocalHost().getHostName();
//			System.out.println(temp);
//		} catch (UnknownHostException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
	
		
		
		
//		
//		File file = new File("acts.jar");
//		byte buffer[] = new byte[(int)file.length()];
//		try {
//		     BufferedInputStream input = new
//		       BufferedInputStream(new FileInputStream("acts.jar"));
//		     input.read(buffer,0,buffer.length);
//		     input.close();
//		     
//		   //  System.out.println(new String(buffer));
//		} catch (Exception e){
//			System.out.println("wfwf");
//		}
//		
//		
//		File file2 = new File("acts2.jar");
//		BufferedOutputStream output;
//		try {
//			output = new
//				 BufferedOutputStream(new FileOutputStream(file2.getName()));
//	        output.write(buffer,0,buffer.length);
//	        output.flush();
//	        output.close();
//		} catch (FileNotFoundException e2) {
//			// TODO Auto-generated catch block
//			e2.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//		
//
//		try {
//			System.out.println(InetAddress.getLocalHost().getHostAddress());
//		} catch (UnknownHostException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		
//		// TODO Auto-generated method stub
//		Scanner scan = new Scanner(System.in);
//		String filename = scan.nextLine();
//		
//		 
//		FileSpliter spliter;
//		try {
//			spliter = new FileSpliter(filename, (double) (1024*1024*0.01));
//			String data = spliter.getNextBlock();
//			Integer numbering = 0;
//			while (data != null){
//				
//				numbering++;
//				String num = numbering.toString(); 
//				submit("testing_"+num, data);
//				data = new String();
//				data = spliter.getNextBlock();
//				
//				
//				System.out.println("------------------------------------------------------");
//			}
//			
//		} catch (FileNotFoundException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		} catch (IOException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//		
//		
		
		
		

		
		
		
		
		
		
		
		
		
		
	}

	
	private static void runClassName(String className){

		String separator = System.getProperty("file.separator");
		String[] args = new String[4];
		
		args[0] = "java";
		args[1] = "-cp";
		args[2] = "."+separator+"*";
		args[3] = className;
		
		 
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
	public static boolean submit(String filename, String data) throws RemoteException {
		// TODO Auto-generated method stub
		
		BufferedWriter writer = null;
		File file = new File(filename);
		
		
		try {
			if( file.createNewFile()){
				System.out.println("creating new");
				System.out.println(data);
				
				writer = new BufferedWriter(new FileWriter(file));
				writer.write(data);
				System.out.println("File " + filename + " has been created.");
				writer.close();
			} else {
				System.out.println("There are duplicate file names");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println(e.toString());
			return false;
		} finally {
		
			try {
				if(writer !=null ){
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
	

private static void inheritIO(final InputStream src, final PrintStream dest) {
    new Thread(new Runnable() {
        public void run() {
            Scanner sc = new Scanner(src);
            while (sc.hasNextLine()) {
                dest.println(sc.nextLine());
            }
        }
    }).start();
}
//
//public static boolean extraction(String jarFileName, String dictory) throws IOException{
//	
//	 java.util.jar.JarFile jarfile1 = new java.util.jar.JarFile(new java.io.File(jarFileName));
//    java.util.Enumeration<java.util.jar.JarEntry> enu= jarfile1.entries();
//    while(enu.hasMoreElements())
//    {
////        String destdir = "jar/";     //abc is my destination directory
//        java.util.jar.JarEntry je = enu.nextElement();
////
//        System.out.println(je.getName());
//    }
//    
//System.out.println("THis is mine!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1");
////	
//	try {
//		java.util.jar.JarFile jarfile = new java.util.jar.JarFile(new File(jarFileName));
//		java.util.Enumeration<java.util.jar.JarEntry> entry= jarfile.entries();
//	    while(entry.hasMoreElements())
//	    {
//	    	java.util.jar.JarEntry jarEntry = entry.nextElement();
//
//	        System.out.println(jarEntry.getName());
//
//	        File file = new File(dictory, jarEntry.getName());
//	        if(!file.exists())
//	        {
//	        	file.getParentFile().mkdirs();
//	        	file = new java.io.File(dictory, jarEntry.getName());
//	        }
//	        if(jarEntry.isDirectory())
//	        {
//	            continue;
//	        }
//	        InputStream input = jarfile.getInputStream(jarEntry);
//	        FileOutputStream output = new FileOutputStream(file);
//	        while(input.available()>0)
//	        {
//	        	output.write(input.read());
//	        }
////	        output.close();
////	        input.close();
//	        return true;
//	    }
//	
//	return false;
//	
//}
}
