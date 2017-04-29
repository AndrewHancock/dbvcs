package hancock.data.model;

import java.util.Collection;

public class Commit {
	private String fileFormat;
	private String compressionCodec;	
	private Collection<Table> tables;
	
	public Commit(String fileFormat, String compressionCodec, Collection<Table> tables) {
		this.fileFormat = fileFormat;
		this.compressionCodec = compressionCodec;
		this.tables = tables;
	}
	
	public String getFileFormat() {
		return fileFormat;
	}
	
	public String getCompressionCode() {
		return compressionCodec;
	}
	
	public Collection<Table> getTables() {
		return tables;
	}
}
