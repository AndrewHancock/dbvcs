package dbvcs.metadata.model;

import java.util.Set;

public class Table {
	private String tableName;
	private Set<String> fieldNames;
	
	public Table(String tableName, Set<String> fieldNames) {
		this.tableName = tableName;
		this.fieldNames = fieldNames;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public Set<String> getFieldNames() {
		return fieldNames;
	}
	
	
}
