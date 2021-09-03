package com.evolving.nglm.evolution.localteststart;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Scanner;
import java.util.StringTokenizer;

public class GetRunConfigParameters {
	public static void main(String[] args) {
	  
	  // first load the system variables
		// args[0] is the name of the class to filter the process, by example EvolutionEngine
		// args[1] is the name of the container to be mocked, by example ev-evolutionengine_evolutionengine-001
		// args[2] is the customer repository where the process will be started, by exmplae /home/fduclos/workspace/nglm-qa

	   // now get container id to be able to copy repositories from container...
    String containerId = null;
    ArrayList<String>  result = executeCommand("docker container ls | grep " + args[1] + "| cut -c 1-12") ;
    for(String s : result) {
      containerId = s;
        System.out.println("Container Id " + containerId); 
    }

    String processId = null;
    result = executeCommand("docker inspect -f '{{.State.Pid}}' " + containerId);
    for(String s : result) {
      processId = s;
        System.out.println("Java process Id " + processId); 
    }
	  
	  result = executeCommand("ps -ef | grep " + processId + "| grep -v grep");
		String ps = "";
		boolean cp = false; // to skip classpath
		for(String s : result) {

	//		if(!s.startsWith("-Xrunjdwp") && !s.startsWith("-javaagent")) {
      if(!s.startsWith("-Xrunjdwp")) {

				if(cp == false) {
					// not in classpath definition
					ps = ps + " " + s;
					if(s.startsWith("-cp")) {
						cp = true;
					}
				}
				else {
					// this is the classpath entry, let filter the libs
					// filter all libs that are into /app/jars
					StringTokenizer st = new StringTokenizer(s,":");
					String finalcp = null;
				     while (st.hasMoreTokens()) {
				        String next = st.nextToken();
				        if(!next.startsWith("/app/jars")) {
				        	if(finalcp == null) {
				        		finalcp = next;
				        	}
				        	else {
				        		finalcp = finalcp + ":" + next;
				        	}				        	
				        }
				     }					
					cp = false;
					ps = ps + " " + finalcp;					
				}
			}
		}
		
    	
  		// now make the 2 docker cp
    	result = executeCommand("rm -rf " + args[2] + "/tmp/" + args[0] + "/app " + args[2] + "/tmp/" +args[0] + "/etc " + args[2] + "/tmp/" +args[0] + "/usr" );
    	result = executeCommand("mkdir -p " + args[2] +"/tmp/" + args[0]);
    	result = executeCommand("docker cp " + containerId + ":/etc " + args[2] + "/tmp/" + args[0] + "/");
    	result = executeCommand("docker cp " + containerId + ":/app " + args[2] + "/tmp/" + args[0] + "/");
    	result = executeCommand("docker cp " + containerId + ":/usr " + args[2] + "/tmp/" + args[0] + "/");
    	    
	        System.out.println("Brut PS" + ps);
	        ps = ps.substring(ps.indexOf("java")+5);
	        String beforeClassName = ps.substring(0, ps.indexOf(args[0]));
	        beforeClassName = beforeClassName.substring(0, beforeClassName.lastIndexOf(" "));
	        beforeClassName = beforeClassName.replaceAll("/app", args[2] + "/tmp/" + args[0] + "/app");
	        beforeClassName = beforeClassName.replaceAll("/etc", args[2] + "/tmp/" + args[0] + "/etc");
	        beforeClassName = beforeClassName.replaceAll("/usr", args[2] + "/tmp/" + args[0] + "/usr");
	        String afterClassName = ps.substring(ps.indexOf(args[0])+args[0].length() + 1, ps.length());
	        afterClassName = afterClassName.replaceAll("/app", args[2] + "/tmp/" + args[0] + "/app");
	        afterClassName = afterClassName.replaceAll("/etc", args[2] + "/tmp/" + args[0] + "/etc");
	        afterClassName = afterClassName.replaceAll("/usr", args[2] + "/tmp/" + args[0] + "/usr");
	        
	        System.out.println("VM arguments: " + beforeClassName);
	        System.out.println("Program arguments: " + afterClassName);

	        
	}
	
	private static ArrayList<String> executeCommand(String command) {
        ProcessBuilder builder = new ProcessBuilder( "/bin/bash" );
        Process p=null;
        ArrayList<String> result = new ArrayList<>();
        try {
            p = builder.start();
        }
        catch (IOException e) {
            System.out.println(e);
        }
        //get stdin of shell
        BufferedWriter p_stdin = 
          new BufferedWriter(new OutputStreamWriter(p.getOutputStream()));
        
        try {
            //single execution
        	System.out.println("Execute " + command);
	        p_stdin.write(command);
	        p_stdin.newLine();
	        p_stdin.write("exit");
	        p_stdin.newLine();
	        p_stdin.flush();
        }
        catch (IOException e) {
        	System.out.println(e);
        }

	    // write stdout of shell (=output of all commands)
	    Scanner s = new Scanner( p.getInputStream() );
	    while (s.hasNext())
	    {
	    	String output = s.next();
	    	System.out.println("OUTPUT: " + output);
	    	result.add(output);
	    }
	    s.close();
	    
	    return result;
	}
}

		
		
	

