package rhss.common;

public class utilities {
	public utilities(){}
	
	public boolean isNumeric(String input){
        try{
        	Integer.parseInt(input);
        	return true;
        }
        catch(Exception e){
        	return false;
        }
    }
    
   public String getYrOfExperience(String input){
    	StringBuilder yrOfExp = new StringBuilder();    	
    	if(input.length()>0){
	    	for(int i=10; i<input.length(); i++)
	    	{
	    		if(Character.isDigit(input.charAt(i))){
	    			yrOfExp.append(input.charAt(i));    			
	    		}
	    	}  
    	}
    	return yrOfExp.toString();
    }
	
	//get as long as number
	public long getSalary(String input){
		long salary = 0;
    	StringBuilder expectedSalary = new StringBuilder(); 
        
    	if(input.length()>0){
    		input.replace("salary", "");
    		input.replace(":", "");
    	    		
	    	for(int i=0; i<input.trim().length(); i++)
	    	{
	    		if(Character.isDigit(input.charAt(i)) || input.charAt(i)=='.'){
	    			
	    			expectedSalary.append(input.charAt(i)); 
	    		}
	    		if(input.charAt(i)=='-' && expectedSalary.length()>0)
	    		{
	    			System.out.println("Util input salary:"+input);
	    			if(input.contains("k")){			    	
				    	String tempSalary = expectedSalary.toString();
				    	tempSalary = tempSalary.replace('k',' ');
				    	tempSalary = tempSalary.trim();
				    	Double temp = Double.valueOf(tempSalary.toString())*1000;
				        System.out.println("Util temp salary:"+temp);
				    	String s =temp.toString();
				    	s = s.replaceAll((String)"\\.0$", "");
				    	salary = Long.valueOf(s);
	    			}
	    			else
	    			{
	    			salary = Long.valueOf(expectedSalary.toString());
	    			}
				    	return salary;			 	 	
	    		}
	    	}
	    	try{
			if(input.contains("k")){			    	
			    	String tempSalary = expectedSalary.toString();
			    	tempSalary = tempSalary.replace('k',' ');
			    	tempSalary = tempSalary.trim();
			    	Double temp = Double.valueOf(tempSalary.toString())*1000;
			    	String s =temp.toString();
			    	s = s.replaceAll((String)"\\.0$", "");
			    	salary = Long.valueOf(s);
			    	return salary;
		 	}			
		    	salary = Long.valueOf(expectedSalary.toString());
	    	}
	    	catch(Exception e)
	    	{
	    		 
	    	}
    	}  
		return salary;
	}
}

	

