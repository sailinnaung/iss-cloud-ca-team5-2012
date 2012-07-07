package rhss.mapreduce;

import java.io.IOException;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import rhss.common.Constants;
import rhss.dataaccess.FileAccess;
import rhss.dataaccess.FileAccessImpl;

/**
 * SkillSetMapper Maps each observed word in a line to a (filename) string.
 */
public class SkillSetMapper extends Mapper<LongWritable, Text, Text, Text> {

	FileAccess fAccess = new FileAccessImpl();
    private Set<String> tagoreStopwords;
    
    public SkillSetMapper() {
    	tagoreStopwords = fAccess.getTagoreStopwords(Constants.SKILL_SET);
    }
    
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Compile all the words using regex
        Pattern p = Pattern.compile("\\w*[#.]*\\w*");
        Matcher m = p.matcher(value.toString());

        // Get the name of the file from the inputsplit in the context
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        // build the values and write <k,v> pairs through the context
        StringBuilder valueBuilder = new StringBuilder();
        
        while (m.find()) {
            String matchedKey = m.group().trim().toLowerCase(); 
            
            if (tagoreStopwords.contains(matchedKey)) { 
	            valueBuilder.append(fileName);
	            context.write(new Text(matchedKey), new Text(valueBuilder.toString()));
            }
            
            valueBuilder.setLength(0);
        }
    }
}
