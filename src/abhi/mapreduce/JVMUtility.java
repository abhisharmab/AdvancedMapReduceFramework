package abhi.mapreduce;

import java.io.File;

//Reference: http://docs.oracle.com/javase/7/docs/api/java/lang/ProcessBuilder.html#ProcessBuilder(java.lang.String...)

public class JVMUtility {

  public static void startProcessinJVM(String[] args) throws Exception {
	
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    
    String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
    String[] processbuilderArgs = new String[args.length + 4];
    processbuilderArgs[0] = path;
    processbuilderArgs[1] = "-cp";
    processbuilderArgs[2] = classpath;

    //Provide the MapReduce Jar Folder ensure everyone has RMI access. 
    //Just like Douglas Runs the Code Manually.
    processbuilderArgs[3] = "-Djava.rmi.server.codebase=file:" + SystemConstants.getConfig(SystemConstants.RMI_CODE_BASE);
    
    for (int i = 4, j = 0; j < args.length; i++, j++) {
    	processbuilderArgs[i] = args[j];
    }

    //Start a brand new process -- under a new JVM
    ProcessBuilder processBuilder = new ProcessBuilder(processbuilderArgs);
    processBuilder.start();
  }
}
