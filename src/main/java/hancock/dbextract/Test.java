package hancock.dbextract;


import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import hancock.dbextract.metadata.jdbc.oracle.OracleMetadataExtractor;
import hancock.dbextract.model.Table;

public class Test {
	private static Gson gson = new GsonBuilder().setPrettyPrinting().create();		
	
	private static void writeTableMetadata(Path filePath, Collection<Table> tables) throws IOException {
		try(Writer writer = new FileWriter(filePath.toString())) {
			gson.toJson(tables, writer);
		}
	}
	
	private static Collection<Table> readTableMetadata(Path filePath) throws IOException {
		byte[] encoded = Files.readAllBytes(filePath);		  
		String json =   new String(encoded, Charset.defaultCharset());		
		
		Type setType = new TypeToken<Collection<Table>>() {}.getType();
		return gson.fromJson(json, setType);		
	}
	
	private static void createTargetDirectory(String targetDir) throws Exception {
		if(Files.isDirectory(Paths.get(targetDir))) {
			throw new Exception("Target directory already exists.");
		}
		else {
			Files.createDirectory(Paths.get(targetDir));
		}
	}	

	public static void main(String[] args) throws Exception {
		createTargetDirectory(args[1]);
		Collection<Table> tables = readTableMetadata(Paths.get(args[0]));
		Collection<Table> tableMetadata = OracleMetadataExtractor.extractMetadata("jdbc:oracle:thin:@192.168.1.232:50001:xe", tables);
		writeTableMetadata(Paths.get(args[1], "metadata.json"), tableMetadata);
		
		
		SparkSession spark =  SparkSession.builder()
				.master("local")
				.appName("SparkCSVExample")
				.config("spark.some.config.option", "some-value")
				.getOrCreate();
		
		tableMetadata.forEach(table -> {
			Dataset<Row> employeeTable = spark.read()
				.format("jdbc")
				.option("url", "jdbc:oracle:thin:@192.168.1.232:50001:xe")
				.option("dbtable", table.getTableName())
				.option("user", "hr")
				.option("password", "test")
				.load();
			
			employeeTable.write()
				.option("header", true)
				.csv(Paths.get(args[1], table.getTableName()).toString());
		});
	}



}
