package dbvcs.metadata.model;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class Table {
	private String tableName;
	private Set<String> keyFieldNames;
	private Collection<String> fieldNames;
	
	public Table(String tableName, Collection<String> fieldNames) {
		this.tableName = tableName;
		this.fieldNames = fieldNames;
	}
	
	public String getTableName() {
		return tableName;
	}

	public Collection<String> getFieldNames() {
		return fieldNames;
	}
	
	public Collection<String> getKeyFieldNames() {
		return keyFieldNames;
	}
	
	public String toString() {
		return tableName + "(" + (fieldNames != null ? fieldNames.stream().collect(Collectors.joining(", ")): "") + ")";
	}
}
