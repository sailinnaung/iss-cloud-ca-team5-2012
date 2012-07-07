package rhss.dataaccess;


import java.util.Map;
import java.util.Set;



public class FileAccessImpl implements FileAccess{
	
	public Set<String> getTagoreStopwords(String key)
	{
		Map<String, Set<String>> map = FileHandler.getKeyValueInputCriteria();
		
		return map.get(key);
	}
	
	public void deleteUnWantedFiles()
	{	
		FileHandler.deleteUnWantedFiles();
	}	
}
