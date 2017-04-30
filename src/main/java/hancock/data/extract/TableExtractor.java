package hancock.data.extract;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.model.Table;

public class TableExtractor {
	private SparkSession spark;
	private String uri;
	private String format;
	private String compression;

	public TableExtractor(SparkSession spark, String uri, String format, String compression) {
		this.spark = spark;
		this.uri = uri;
		this.format = format;
		this.compression = compression;
	}
	
	private void writeCsv(Dataset<Row> dataset, Path outputPath) {
		DataFrameWriter<Row> writer = 
				dataset.write();
		
		if(compression != null) {
			writer = writer.option("compression ", compression);
		}
		writer.csv(outputPath.toString());
	}

	public void extractTable(Table schema, Path outputPath) {
		Dataset<Row> dataset = spark.read()
			.jdbc(uri, schema.getTableName(), new Properties());
		
		writeCsv(dataset, outputPath);
	}
}
