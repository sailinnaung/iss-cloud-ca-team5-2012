package rhss.dataaccess;

import java.util.Set;

public interface FileAccess {
	
	Set<String> getTagoreStopwords(String key);
	
	void deleteUnWantedFiles();	
}
