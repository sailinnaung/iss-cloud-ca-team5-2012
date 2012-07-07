package rhss.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import rhss.common.Constants;
import rhss.common.utilities;
import rhss.dataaccess.FileAccess;
import rhss.dataaccess.FileAccessImpl;

/**
 * LineIndexMapper Maps each observed word in a line to a (filename@offset) string.
 */
public class ExperienceMapper extends Mapper<LongWritable, Text, Text, Text> {

    private static Set<String> tagoreStopwords;
    private static int requiredYear;
    private int resumeYear=0;

    static {
        tagoreStopwords = new HashSet<String>();
        tagoreStopwords.add("experiences");
        tagoreStopwords.add("experiences:");
        tagoreStopwords.add("experiences-"); 
        tagoreStopwords.add("experience"); 
        tagoreStopwords.add("experience:");
        tagoreStopwords.add("experience-"); 
        tagoreStopwords.add("experiences :"); 
    }  
    
    FileAccess fAccess = new FileAccessImpl();

    public ExperienceMapper() { 
    	
    	Set<String> setExperince = fAccess.getTagoreStopwords(Constants.EXPERIENCE);    	
    	String[] arrExperince = new String[]{};
    	arrExperince = setExperince.toArray(new String[0]);
    	
    	if(arrExperince.length > 0)
    		requiredYear = Integer.valueOf(arrExperince[0]);
    	else
    		requiredYear = 1; //default year assignment;
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        utilities uti = new utilities();
    	
        Pattern p = Pattern.compile("\\p{Punct}");
        Matcher m = p.matcher(value.toString()); 
        
        StringTokenizer str = new StringTokenizer(value.toString());        

        
        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        StringBuilder valueBuilder = new StringBuilder();
  
        while(str.hasMoreTokens())
        {
            String matchedKey = str.nextToken().toLowerCase();
            
          //For with space case Eg; "Experiences : 10Years"
          //  else (matchedKey.contains("experience")){
              if (tagoreStopwords.contains(matchedKey)){
            	  
            	  //int pos = strLine.indexOf(matchedKey)
            	
            try
            {
            	  
               String year = "yearOfExp";      	  
        	   while(!uti.isNumeric(year))
        	   {    
    		    	year = str.nextToken().toString().toLowerCase(); 
        		    
        		    //remove years that is followed by year of Experience
        		    year = year.replace("years", "");
	            	year = year.replace("year", "");
	            	year = year.replace(":","");
	            	year = year.replace("-","");
	            	year = year.replace("_","");
	            	
	        		if(uti.isNumeric(year)){  	        			
	        			
	   	            	int yrOfExp = Integer.parseInt(year);            	
	   	            	if(yrOfExp>=requiredYear)
	   	            	{
	   	            		valueBuilder.append(fileName);
	   	                    context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
	   	            	}
	        	   } 
        		    
        	    } 
            }
            catch (Exception e)
            {
            	//System.out.println("e.getMessage() ==> " + e.getMessage());
            }
              }
              
              //For without space case Eg; "Experiences:10Years"
              else if((!tagoreStopwords.contains(matchedKey)) && matchedKey.contains("experience") && matchedKey.length()>11)
              {
            	  if(uti.getYrOfExperience(matchedKey).length()>0)
            	  {
  	            	  resumeYear = Integer.parseInt(uti.getYrOfExperience(matchedKey));
  	            	  if(resumeYear >= requiredYear)
  		              {
  		            		valueBuilder.append(fileName);
  		                    context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
  		              }
            	  }
              }   	
               
              valueBuilder.setLength(0);          
        }           
    }            
}
