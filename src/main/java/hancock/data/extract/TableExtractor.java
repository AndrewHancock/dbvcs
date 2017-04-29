package hancock.data.extract;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;

import hancock.data.model.Table;

public class TableExtractor {
	private SparkSession spark;
	private String uri;

	public TableExtractor(SparkSession spark, String uri) {
		this.spark = spark;
		this.uri = uri;
	}

	public void extractTable(Table schema, Path outputPath) {
		spark.read()
			.jdbc(uri, schema.getTableName(), new Properties())
			.write()
			.option("header", true)
			.csv(outputPath.toString());		
	}
}
