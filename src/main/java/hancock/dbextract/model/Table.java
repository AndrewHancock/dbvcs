package hancock.dbextract.model;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class Table {
	private String tableName;
	private Set<String> keyFieldNames;
	private Collection<Field> fields;
	
	public Table(String tableName, Collection<Field> fields) {
		this.tableName = tableName;
		this.fields = fields;
	}
	
	public String getTableName() {
		return tableName;
	}
	
	public Collection<String> getKeyFieldNames() {
		return keyFieldNames;
	}
	
	public Collection<Field> getFields() {
		return fields;
	}
	
	public String toString() {
		return tableName + "(" + (fields != null ? fields.stream().map(f -> f.toString()).collect(Collectors.joining(", ")): "") + ")";
	}
}
