package hancock.data.compare;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.compare.cli.CompareCli;
import hancock.data.compare.cli.CompareOptions;
import hancock.data.model.Table;
import hancock.data.model.serialization.JsonTableSerializer;

public class DataCompare {
	public static void main(String[] args) throws IOException {
		Options cliOptions  = CompareCli.getOptions();
		CompareOptions options = null;
		try {
			options = CompareCli.parseOptions(args, cliOptions);
		}
		catch(ParseException e) {
			new HelpFormatter().printHelp("DataCompare", cliOptions);
			System.exit(-1);	
		}		
		
		SparkSession spark =  SparkSession.builder()
				.master("local")
				.appName("Data Compare")				
				.getOrCreate();				
		
		Collection<Table> leftSchema = JsonTableSerializer.readTableMetadata(Paths.get(options.getLeftPath(), "metadata.json"));
		Collection<Table> rightSchema = JsonTableSerializer.readTableMetadata(Paths.get(options.getRightPath(), "metadata.json"));

		for(Table table : leftSchema) {
			Dataset<Row> leftTable = spark.read().csv(Paths.get(options.getLeftPath(), table.getTableName()).toString());
			Dataset<Row> rightTable = spark.read().csv(Paths.get(options.getRightPath(), table.getTableName()).toString());
			
			leftTable.except(rightTable).write().csv(Paths.get(options.getOutputPath(), table.getTableName() + "_DELETE").toString());
			rightTable.except(leftTable).write().csv(Paths.get(options.getOutputPath(), table.getTableName() + "_ADD").toString());
			
			leftTable.joinWith(rightTable, leftTable.col(leftTable.columns()[0]).equalTo(rightTable.col(rightTable.columns()[0]))).show();
		}
				

	}

}
