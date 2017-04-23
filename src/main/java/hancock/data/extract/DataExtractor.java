package hancock.data.extract;


import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import hancock.data.extract.cli.ExtractCli;
import hancock.data.extract.cli.ExtractOptions;
import hancock.data.extract.jdbc.oracle.OracleMetadataExtractor;
import hancock.data.model.Table;

public class DataExtractor {
	
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
		
		Options cliOptions = ExtractCli.getCliOptions();
		ExtractOptions options = null;
		try {
			
			options = ExtractCli.parseArguments(args, cliOptions);
		}
		catch (ParseException e) {
			new HelpFormatter().printHelp("DataExtractor", cliOptions);
			System.exit(-1);
		}
		
		createTargetDirectory(options.getOutputDirectory());
		Collection<Table> tables = readTableMetadata(Paths.get(options.getCataloguePath()));
		Collection<Table> tableMetadata = OracleMetadataExtractor.extractMetadata(options.getJdbcPath(), tables);
		writeTableMetadata(Paths.get(options.getOutputDirectory(), "metadata.json"), tableMetadata);
		
		
		SparkSession spark =  SparkSession.builder()
				.master("local")
				.appName("SparkCSVExample")				
				.getOrCreate();
		
		for(Table table : tableMetadata) {		
			Dataset<Row> employeeTable = spark.read()
				.format("jdbc")
				.option("url", options.getJdbcPath())
				.option("dbtable", table.getTableName())
				.option("user", "hr")
				.option("password", "test")
				.load();
			
			employeeTable.write()
				.option("header", true)
				.csv(Paths.get(options.getOutputDirectory(), table.getTableName()).toString());
		}
	}



}
