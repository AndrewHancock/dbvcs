package hancock.data.compare;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Collection;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import hancock.data.compare.cli.CompareOptions;
import hancock.data.dataframe.DataFrameFactory;
import hancock.data.model.Commit;
import hancock.data.model.Table;
import hancock.data.model.serialization.JsonTableSerializer;

public class Compare {
	private SparkSession spark;
	private CompareOptions options;
	private Commit leftManifest;
	private Commit rightManifest;
	
	public Compare(SparkSession spark, CompareOptions options) throws IOException {
		this.spark = spark;
		this.options = options;
		leftManifest = JsonTableSerializer.readCommitJson(Paths.get(options.getLeftPath(), "metadata.json"));
		rightManifest = JsonTableSerializer.readCommitJson(Paths.get(options.getRightPath(), "metadata.json"));
	}
	
	public void comapreCommits() {
		DataFrameFactory leftDataFrameFactory = new DataFrameFactory(spark, options.getLeftPath(), leftManifest);
		DataFrameFactory rightDataFrameFactory = new DataFrameFactory(spark, options.getRightPath(), rightManifest);
		for(Table table : leftManifest.getTables()) {
			Dataset<Row> leftTable = leftDataFrameFactory.getDataFrame(table);
			Dataset<Row> rightTable = rightDataFrameFactory.getDataFrame(table);			
			
			Column joinCondition = buildJoinCondition(table.getKeyFieldNames(), leftTable, rightTable);
			
			leftTable.joinWith(rightTable, joinCondition).show();
		}
	}
	
	private static Column buildJoinCondition (Collection<String> keyFields, Dataset<Row> leftTable, Dataset<Row> rightTable) {
		Column predicate = null;
		for(String field : keyFields) {
			Column fieldEq = leftTable.col(field).equalTo(rightTable.col(field));
			
			if(predicate == null) {
				predicate = fieldEq;
			}
			else {
				predicate = predicate.and(fieldEq);
			}
		}
		return predicate;
	}
}
