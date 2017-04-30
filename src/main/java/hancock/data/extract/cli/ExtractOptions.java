package hancock.data.extract.cli;

public class ExtractOptions {
	private String jdbcPath;
	private String outputDirectory;
	private String cataloguePath;
	private String format;
	private String compressionCodec;
	
	public String getJdbcPath() {
		return jdbcPath;
	}
	
	public void setJdbcPath(String jdbcPath) {
		this.jdbcPath = jdbcPath;
	}
	
	public String getOutputDirectory() {
		return outputDirectory;
	}
	
	public void setOutputDirectory(String outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	
	public String getCataloguePath() {
		return cataloguePath;
	}
	
	public void setCataloguePath(String cataloguePath) {
		this.cataloguePath = cataloguePath;
	}

	public String getFormat() {
		return format;
	}

	public void setFormat(String format) {
		this.format = format;
	}

	public String getCompressionCodec() {
		return compressionCodec;
	}

	public void setCompressionCodec(String compressionCodec) {
		this.compressionCodec = compressionCodec;
	}	
}
