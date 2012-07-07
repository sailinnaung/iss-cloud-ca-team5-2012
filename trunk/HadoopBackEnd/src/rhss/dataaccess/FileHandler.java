package rhss.dataaccess;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import rhss.common.Constants;

public class FileHandler {
	
    /**
     * Google's search Stopwords
     */
    private static Set<String> getTagoreStopwords(String key){
    	
    	Map<String, Set<String>> map = new HashMap<String, Set<String>>();
    	map = getKeyValueInputCriteria();
        
        Set<String> tagoreStopwords = new HashSet<String>();
        tagoreStopwords = map.get(Constants.SKILL_SET);
        
        return tagoreStopwords;
    }
	
	public static Map<String, Set<String>> getKeyValueInputCriteria()
	{		
		Map<String, Set<String>> map = new HashMap<String, Set<String>>();
        FileInputStream fStream = null;
        DataInputStream dInputStream = null;
        InputStreamReader inReader = null;
        BufferedReader bReader = null;
        
        try
        {
	        String path = new java.io.File(".").getCanonicalPath();
	        fStream = new FileInputStream(path + "/InputCriteria/" + Constants.FILE_NAME);
	        dInputStream = new DataInputStream(fStream);
	        inReader = new InputStreamReader(dInputStream);
	        bReader = new BufferedReader(inReader);	   
	        
	        String lineData = null;
	        int count = 0;
	        
	        while ((lineData = bReader.readLine()) != null) {
	        	
	        	String key = null;
	        	Set<String> setValue = new HashSet<String> ();
	        	
	                count++;
	                if (lineData != null) {
	                	
	                	if(count == 1)                         	
	                		key = Constants.SKILL_SET;                        	
	                	else if(count == 2)
	                		key = Constants.EXPERIENCE;
	                	else
	                		key = Constants.SALARY;
	                		
	                	StringTokenizer token = new StringTokenizer(lineData, Constants.TEXT_DATA_DELIMITER);
	                	               		                	
	                	while(token.hasMoreTokens())
	                	{
	                		setValue.add(token.nextToken());                                
	                	}
	                }
	                
	            map.put(key, setValue);
	                
		    }
        }catch (FileNotFoundException e) {
	            e.printStackTrace();
	    } catch (IOException e) {
	            e.printStackTrace();
	    } catch (Exception e) {
	            e.printStackTrace();
	    } finally {
	            if (bReader != null) {
	                    try {
	                            bReader.close();
	                    } catch (IOException e) {
	                            e.printStackTrace();
	                    }
	            }
	
	            if (inReader != null) {
	                    try {
	                            inReader.close();
	                    } catch (IOException e) {
	                            e.printStackTrace();
	                    }
	            }
	
	            if (dInputStream != null) {
	                    try {
	                    	dInputStream.close();
	                    } catch (IOException e) {
	                            e.printStackTrace();
	                    }
	            }
	            if (fStream != null) {
	                    try {
	                            fStream.close();
	                    } catch (IOException e) {
	                            e.printStackTrace();
	                    }
	            }
	    }
		
		return map;	
	}

	public static void deleteUnWantedFiles()
	{		
		FileInputStream fStream = null;
        DataInputStream dInputStream = null;
        InputStreamReader inReader = null;
        BufferedReader bReader = null;
        
        try
        {
	        String path = new java.io.File(".").getCanonicalPath();
	        fStream = new FileInputStream(path + "/" + Constants.OUTPUT_PATH + "/" + Constants.OUTPUT_FILENAME);
	        dInputStream = new DataInputStream(fStream);
	        inReader = new InputStreamReader(dInputStream);
	        bReader = new BufferedReader(inReader);		        
			
			Set<String> setName = new HashSet<String>();		
            String line;
            
            while ((line=bReader.readLine()) != null){
            	
            	StringTokenizer tokenizer = new StringTokenizer(line, "\t");	            	
            	
    			while (tokenizer.hasMoreTokens()) {	
    				
    				String str = tokenizer.nextToken();
    				if(str.contains(".txt"))
    				{
    					StringTokenizer token = new StringTokenizer(str, ",");
    					
    					while(token.hasMoreTokens())
    					{
    						setName.add(token.nextToken());
    					}
    				}
    			}    			
            }
				
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            
			//get list of existing files from the INPUT_PATH directory
	        Path inPath = new Path(Constants.INPUT_PATH);
			FileStatus[] status = fs.listStatus(inPath);
			Path[] listedPaths = FileUtil.stat2Paths(status);
			String workPath = fs.getWorkingDirectory() + "/" + Constants.INPUT_PATH + "/";			
			
			System.out.println("workPath ==> " + workPath);
			
			for (Path p : listedPaths) {	
				
				//if file is not included in the result files set (SetName), then delete
				//so that the remaining files will be the input of 2nd level mad/reducer.				
				if(!setName.contains(p.toString().replace(workPath, "")))
					FileSystem.get(conf).delete(p, true);				
			}
		
		}catch (FileNotFoundException e) {
	        e.printStackTrace();
	    } catch (IOException e) {
	            e.printStackTrace();
	    } catch (Exception e) {
	            e.printStackTrace();
	    } finally {
            if (bReader != null) {
                    try {
                            bReader.close();
                    } catch (IOException e) {
                            e.printStackTrace();
                    }
            }

            if (inReader != null) {
                    try {
                            inReader.close();
                    } catch (IOException e) {
                            e.printStackTrace();
                    }
            }

            if (dInputStream != null) {
                    try {
                    	dInputStream.close();
                    } catch (IOException e) {
                            e.printStackTrace();
                    }
            }
            if (fStream != null) {
                    try {
                            fStream.close();
                    } catch (IOException e) {
                            e.printStackTrace();
                    }
            }
	    }
	    
    }
	
}
