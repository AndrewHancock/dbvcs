package hancock.data.model.serialization;

import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import hancock.data.model.Table;

public class JsonTableSerializer {
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();
	
	public static void writeTableMetadata(Path filePath, Collection<Table> tables) throws IOException {
		try(Writer writer = new FileWriter(filePath.toString())) {
			gson.toJson(tables, writer);
		}
	}
	
	public static Collection<Table> readTableMetadata(Path filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(filePath);		  
		String json =   new String(encoded, Charset.defaultCharset());		
		
		Type setType = new TypeToken<Collection<Table>>() {}.getType();
		return gson.fromJson(json, setType);		
	}
}
