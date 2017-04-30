package hancock.data.compare;

import java.io.IOException;
import java.nio.file.Paths;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.compare.cli.CompareCli;
import hancock.data.compare.cli.CompareOptions;
import hancock.data.dataframe.DataFrameFactory;
import hancock.data.model.Commit;
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
		
		Compare compare = new Compare(spark, options);
		compare.comapreCommits();
	}

}
