package dbvcs;


import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.Collection;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import dbvcs.metadata.jdbc.DatasetMetadataExtractor;
import dbvcs.metadata.model.Table;

public class Test {
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();		
	
	private static void writeTableMetadata(String filePath, Collection<Table> tables) throws IOException {
		try(Writer writer = new FileWriter(filePath)) {
			gson.toJson(tables, writer);
		}
	}
	
	private static Collection<Table> readTableMetadata(String filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(Paths.get(filePath));		  
		String json =   new String(encoded, Charset.defaultCharset());		
		
		Type setType = new TypeToken<Collection<Table>>() {}.getType();
		return gson.fromJson(json, setType);		
	}

	public static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, SQLException {
		Collection<Table> tables = readTableMetadata(args[0]);
		Collection<Table> tableMetadata = DatasetMetadataExtractor.extractMetadata("jdbc:oracle:thin:@192.168.1.232:50001:xe", tables);
		writeTableMetadata(args[1], tableMetadata);
	}



}
