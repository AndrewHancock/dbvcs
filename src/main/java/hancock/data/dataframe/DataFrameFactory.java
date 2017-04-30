package hancock.data.dataframe;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.stream.Collectors;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import hancock.data.model.Commit;
import hancock.data.model.Field;
import hancock.data.model.FieldType;
import hancock.data.model.Table;

public class DataFrameFactory {
	private SparkSession spark;
	private String commitPath;
	private Commit manifest;
	
	public DataFrameFactory(SparkSession spark, String commitPath, Commit manifest) {
		this.spark = spark;
		this.commitPath = commitPath;
		this.manifest = manifest;
	}
	
	public Dataset<Row> getDataFrame(Table table) {

		Dataset<Row> dataset = null; 
		String path = Paths.get(commitPath, table.getTableName()).toString();
		if(manifest.getFileFormat().equals("csv")) {
			dataset = spark.read().schema(getCsvSchema(table)).csv(path);
		}
		else if (manifest.getFileFormat().equals("parquet")) {
			dataset = spark.read().parquet(path);
		}
		
		return dataset;
	}
	
	private static DataType getDataType(FieldType type) {
		switch(type) {
		case STRING:
			return DataTypes.StringType;
		case INTEGER: 
			return DataTypes.IntegerType;
		case DATE:
			return DataTypes.DateType;
		case DECIMAL:
			return DataTypes.createDecimalType();
		default:
			throw new RuntimeException("Unrecognized FieldType: " + type);
		}
	}
	
	private StructType getCsvSchema(Table table) {
		ArrayList<StructField> fields = new ArrayList<>(); 
		for(Field field : table.getFields()) {
			fields.add(new StructField(field.getName(), getDataType(field.getType()), true, Metadata.empty()));
		}
		return new StructType(fields.toArray(new StructField[fields.size()]));
	}

}
