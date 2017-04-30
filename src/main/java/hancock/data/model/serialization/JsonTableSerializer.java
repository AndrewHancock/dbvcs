package hancock.data.model.serialization;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import hancock.data.model.Commit;

public class JsonTableSerializer {
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	public static void writeCommitJson(Path filePath, Commit commit) throws IOException {
		try(Writer writer = new FileWriter(filePath.toString())) {
			gson.toJson(commit, writer);
		}
	}
	
	public static Commit readCommitJson(Path filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(filePath);		  
		String json =   new String(encoded, Charset.defaultCharset());
		
		return gson.fromJson(json, Commit.class );		
	}
}
