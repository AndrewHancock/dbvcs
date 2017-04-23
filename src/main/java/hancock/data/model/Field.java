package hancock.data.model;

public class Field {
	private String name;
	private FieldType type;
	
	public Field(String name, FieldType type) {
		this.name = name;
		this.type = type;
	}
	
	public Field(String name) {
		this(name, null);
	}

	public String getName() {
		return name;
	}

	public FieldType getType() {
		return type;
	}
	
	@Override
	public String toString() {
		return name + type.toString();
	}
}
