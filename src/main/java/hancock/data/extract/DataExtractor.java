package hancock.data.extract;


import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.extract.cli.ExtractCli;
import hancock.data.extract.cli.ExtractOptions;
import hancock.data.extract.jdbc.oracle.OracleMetadataExtractor;
import hancock.data.model.Table;
import hancock.data.model.serialization.JsonTableSerializer;

public class DataExtractor {	
	
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
		Collection<Table> tables = JsonTableSerializer.readTableMetadata(Paths.get(options.getCataloguePath()));
		Collection<Table> tableMetadata = OracleMetadataExtractor.extractMetadata(options.getJdbcPath(), tables);
		JsonTableSerializer.writeTableMetadata(Paths.get(options.getOutputDirectory(), "metadata.json"), tableMetadata);		
		
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
