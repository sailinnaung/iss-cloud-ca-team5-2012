package rhss.mapreduce.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import rhss.mapreduce.ExperienceMapper;
import rhss.mapreduce.SalaryMapper;
import rhss.mapreduce.SkillSetMapper;
import rhss.mapreduce.SkillSetReducer;
import rhss.common.*;
import rhss.dataaccess.FileAccess;
import rhss.dataaccess.FileAccessImpl;

	public class RHSSMainClass extends Configured implements Tool {		

		FileAccess fAccess = new FileAccessImpl();
		
		public int run(String[] args) throws Exception {
			
			Configuration conf = getConf();
			int i = 0;			
			
			//-- start first level filtering for Skill Sets --//
			Job jobSkill = new Job(conf, "1st level skill set filtering");

			jobSkill.setJarByClass(RHSSMainClass.class);
			jobSkill.setMapperClass(SkillSetMapper.class);
			jobSkill.setReducerClass(SkillSetReducer.class);

			jobSkill.setOutputKeyClass(Text.class);
			jobSkill.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(jobSkill, new Path(Constants.INPUT_PATH));
			// Erase previous run output (if any)
			FileSystem.get(conf).delete(new Path(Constants.OUTPUT_PATH), true);
			FileOutputFormat.setOutputPath(jobSkill, new Path(Constants.OUTPUT_PATH));				
			
			//-- start to delete the files that are not included after 1st level filtering --//
			if(jobSkill.waitForCompletion(true))
			{				
				fAccess.deleteUnWantedFiles();
			}
			//-- end first level filtering for skill sets --//	
						
			
			//-- start 2nd level filtering for experience --//
			Job jobExperience = new Job(conf, "Line Indexer 1");
			jobExperience.setJarByClass(RHSSMainClass.class);
			jobExperience.setMapperClass(ExperienceMapper.class);
			jobExperience.setReducerClass(SkillSetReducer.class);	
			jobExperience.setOutputKeyClass(Text.class);
			jobExperience.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(jobExperience, new Path(Constants.INPUT_PATH));
			// Erase previous run output (if any)
			FileSystem.get(conf).delete(new Path(Constants.OUTPUT_PATH), true);
			FileOutputFormat.setOutputPath(jobExperience, new Path(Constants.OUTPUT_PATH));	
			
			if(jobExperience.waitForCompletion(true))
			{				
				fAccess.deleteUnWantedFiles();
			}			
			//-- end 2nd level filtering for experience --//				
			
			
			//-- start 3nd level filtering for salary--//
			Job jobSalary = new Job(conf, "Line Indexer 1");
			jobSalary.setJarByClass(RHSSMainClass.class);
			jobSalary.setMapperClass(SalaryMapper.class);
			jobSalary.setReducerClass(SkillSetReducer.class);	
			jobSalary.setOutputKeyClass(Text.class);
			jobSalary.setOutputValueClass(Text.class);
		    FileInputFormat.addInputPath(jobSalary, new Path(Constants.INPUT_PATH));
			// Erase previous run output (if any)
			FileSystem.get(conf).delete(new Path(Constants.OUTPUT_PATH), true);
			FileOutputFormat.setOutputPath(jobSalary, new Path(Constants.OUTPUT_PATH));
	        
			//-- start to delete the files that are not included after 1st level filtering --//
			if(jobSalary.waitForCompletion(true))
			{				
				fAccess.deleteUnWantedFiles();
			}
			//-- end 3rd level filtering for salary --//
			
			
			return i;
		}

		public static void main(String[] args) throws Exception {
			int res = ToolRunner.run(new Configuration(), new RHSSMainClass(), args);
			System.exit(res);
		}
	}
