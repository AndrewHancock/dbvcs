package hancock.data.extract;

import java.nio.file.Path;
import java.util.Properties;

import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.model.Commit;
import hancock.data.model.Table;

public class TableExtractor {
	private SparkSession spark;
	private String uri;
	private Commit commit;
	
	public TableExtractor(SparkSession spark, String uri, Commit commit) {
		this.spark = spark;
		this.uri = uri;
		this.commit = commit;
	}
	
	private void writeCsv(Dataset<Row> dataset, Path outputPath) {
		DataFrameWriter<Row> writer = 
				dataset.write();
		
		if(commit.getCompressionCodec() != null) {
			writer = writer.option("compression ", commit.getCompressionCodec());
		}
		writer.csv(outputPath.toString());
	}
	
	private void writeParquet(Dataset<Row> dataset, Path outputPath) {
		DataFrameWriter<Row> writer = dataset.write();
		
		writer.parquet(outputPath.toString());
	}

	public void extractTable(Table schema, Path outputPath) {
		Dataset<Row> dataset = spark.read()
			.jdbc(uri, schema.getTableName(), new Properties());
		
		if(commit.getFileFormat().toLowerCase().equals("parquet")) {
			writeParquet(dataset, outputPath);	
		}
		else if (commit.getFileFormat().toLowerCase().equals("csv")) {
			writeCsv(dataset, outputPath);
		}
	}
}
