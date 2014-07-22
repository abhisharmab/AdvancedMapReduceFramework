/**
 * 
 */
package abhi.adfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import abhi.mapreduce.SystemConstants;

/**
 * @author Douglas Rew
 * The class will handle the extraction of the JAR file.
 */
public class JarExtraction {
	private String jarFileName;    
	private String directory;
	private String jar_dic;
	
	public JarExtraction(String jarFileName){
		this.jarFileName = jarFileName;
		directory = SystemConstants.getConfig(SystemConstants.JAR_DIRECTORY)+System.getProperty("file.separator");
		jar_dic = directory;
		
		String jarName = jarFileName.substring(0, jarFileName.indexOf("."));
		directory = directory + jarName + System.getProperty("file.separator");
	}
	
	// This code has been referenced from 
	// http://stackoverflow.com/questions/1529611/how-to-write-a-java-program-which-can-extract-a-jar-file-and-store-its-data-in-s
	// This will open the jarfile enumerate through it and create the files with the location that we provide.
	// Extraction of the jar file will happen with Manager/TaskTracker(Mapper,Reducer)
	public boolean extraction(){
		
		 
		try {
			String fileNameWithPath = jar_dic + jarFileName;
			JarFile jarfile = new JarFile(new File(fileNameWithPath));
		    Enumeration<JarEntry> entry= jarfile.entries();
		    while(entry.hasMoreElements())
		    {
		        JarEntry jarEntry = entry.nextElement();

		        System.out.println(jarEntry.getName());

		        File file = new File(directory, jarEntry.getName());
		        if(!file.exists())
		        {
		        	file.getParentFile().mkdirs();
		        	file = new java.io.File(directory, jarEntry.getName());
		        }
		        if(jarEntry.isDirectory())
		        {
		            continue;
		        }
		        InputStream input = jarfile.getInputStream(jarEntry);
		        FileOutputStream output = new FileOutputStream(file);
		        while(input.available()>0)
		        {
		        	output.write(input.read());
		        }
		        output.close();
		        input.close();
		     
		    }
		    return true;
		} catch (IOException e) {
			System.out.println("Error while accessing the Jarfile : " + jarFileName);
			System.out.println("Please Check and try again.");
			e.printStackTrace();
			return false;
		}
		
	}

	public String getJarFileName() {
		return jarFileName;
	}

	public void setJarFileName(String jarFileName) {
		this.jarFileName = jarFileName;
	}

	public String getDirectory() {
		return directory;
	}

	public void setdirectory(String directory) {
		this.directory = directory;
	}
	

}
