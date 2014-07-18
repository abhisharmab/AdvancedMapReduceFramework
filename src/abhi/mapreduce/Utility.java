package abhi.mapreduce;

import java.io.File;


public class Utility {

  public static void startProcessinJVM(String[] args, int jid) throws Exception {
    /* build arguments */
    String separator = System.getProperty("file.separator");
    String classpath = System.getProperty("java.class.path");
    String path = System.getProperty("java.home") + separator + "bin" + separator + "java";
    String[] newargs = new String[args.length + 4];
    newargs[0] = path;
    newargs[1] = "-cp";
    newargs[2] = classpath ;//+ File.pathSeparator + SystemConstants.getConfig(SystemConstants.USER_CLASS_PATH) + separator + "job" + jid;

    /* Get the RMI CODEBASE path */
    newargs[3] = "-Djava.rmi.server.codebase=file:" + SystemConstants.getConfig(SystemConstants.RMI_CODE_BASE);
    for (int i = 4, j = 0; j < args.length; i++, j++) {
      newargs[i] = args[j];
    }

    //Start a brand new process -- under a new JVM
    ProcessBuilder processBuilder = new ProcessBuilder(newargs);
    processBuilder.start();
  }
}
