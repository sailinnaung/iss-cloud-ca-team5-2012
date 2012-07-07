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

public class SalaryMapper extends Mapper<LongWritable, Text, Text, Text> {

	private static Set<String> tagoreStopwords;
    private static Long offeredSalary;
    private long resumeMinSalary=0;

    static {
        tagoreStopwords = new HashSet<String>();
        tagoreStopwords.add("salary");
        tagoreStopwords.add("salary:");
        tagoreStopwords.add("salary-"); 
        tagoreStopwords.add("salary :");
        tagoreStopwords.add("salary -"); 
    }    
    
    FileAccess fAccess = new FileAccessImpl();
    
    public SalaryMapper() { 
    	
    	Set<String> setExperince = fAccess.getTagoreStopwords(Constants.SALARY);    	
    	String[] arrExperince = new String[]{};
    	arrExperince = setExperince.toArray(new String[0]);
    	
    	if(arrExperince.length > 0)
    		offeredSalary = Long.valueOf(arrExperince[0]);
    	else
    		offeredSalary = Long.valueOf(1000);
    }
	
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	utilities util = new utilities();   	 
    	
    	
        Pattern p = Pattern.compile("\\p{Punct}");
        Matcher m = p.matcher(value.toString());       
        
        StringTokenizer str = new StringTokenizer(value.toString());        

        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // build the values and write <k,v> pairs through the context
        StringBuilder valueBuilder = new StringBuilder();
        
        try{
        while(str.hasMoreTokens())
        {
        	 String matchedKey = str.nextToken().toLowerCase();
        	 
        	//For with space case Eg; "Salary :$4000", "Salary: 4.5k"
        	 if (tagoreStopwords.contains(matchedKey)){
	               String salary = "expectedSalary";      	  
	          	   while(!util.isNumeric(salary))
	          	   {
	          		    salary = str.nextToken().toString();
	          		    
	          		    //remove years that is followed by year of Experience
	          		    //salary = salary.replace("S$", "");
		          		//salary = salary.replace("$", "");
		          		//salary = salary.replace(":","");
		          		//salary = salary.replace("-","");
		          		//salary = salary.replace("_","");
		          		System.out.println("space case input to util is:"+ salary);
		          		
	  	        		if(util.getSalary(salary)>0){            	
	  	   	            	resumeMinSalary = util.getSalary(salary); 
	  	   	            	System.out.println("space case:"+ resumeMinSalary);
	  	   	           	    if(offeredSalary >= resumeMinSalary)
	  	   	                {
	  	   	           	System.out.println("space case If is ok:"+ resumeMinSalary);
	  	   	            		valueBuilder.append(fileName);
	  	   	                    context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
	  	   	                    break;
	  	   	             	}
	  	        	   }        
	          	    }
        	     }
             
	          	//For without space case Eg; "Salary:4000", "Salary:4.5k"   
        	     else if((!tagoreStopwords.contains(matchedKey)) && matchedKey.contains("salary") && matchedKey.length()>8){          	   
        	    	
        	    	 //String salary = "expectedSalary";   	  
     	            StringBuilder input = new StringBuilder();
     	            input.append(matchedKey); 
     	               	            
     	           System.out.println("without space case coming to input is:"+input);
             	 	if(util.getSalary(input.toString())>0)
                  	  {
             	 		  resumeMinSalary = util.getSalary(input.toString());
             	 		  System.out.println("Resume Salray without space is: " + resumeMinSalary);
        	            	  if(offeredSalary >= resumeMinSalary)
        		              {
        		            		valueBuilder.append(fileName);
        		                    context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
        		                    break;
        		              }
                  	  }            	 	          		    		     
                    }                               
        }
        valueBuilder.setLength(0);        
    }catch(Exception e)
    {
    	System.err.println("Come to Exception Error is:" +e.getMessage());
    }
    }
}
