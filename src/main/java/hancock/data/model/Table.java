package hancock.data.model;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class Table implements Serializable {
	private static final long serialVersionUID = 1L;

	private String tableName;
	private Set<String> keyFieldNames;
	private Collection<Field> fields;
	private Map<String, Integer> fieldIndexMap;

	public Table(String tableName, Set<String> keyFieldNames, Collection<Field> fields) {
		this.tableName = tableName;
		this.keyFieldNames = keyFieldNames;
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
		return tableName + "("
				+ (fields != null ? fields.stream().map(f -> f.toString()).collect(Collectors.joining(", ")) : "")
				+ ")";
	}

	public int getFieldIndex(String fieldName) {
		if (fieldIndexMap == null) {
			fieldIndexMap = new HashMap<>();
			Iterator<Field> iterator = fields.iterator();
			for (int i = 0; i < fields.size(); i++) {
				fieldIndexMap.put(iterator.next().getName(), i);
			}
		}

		return fieldIndexMap.get(fieldName);
	}
}
