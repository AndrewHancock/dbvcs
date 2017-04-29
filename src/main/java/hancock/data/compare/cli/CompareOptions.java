package hancock.data.compare.cli;

public class CompareOptions {
	private String leftPath;
	private String rightPath;
	private String outputPath;
	private String format;
	private String compressionCodec;
	
	public String getLeftPath() {
		return leftPath;
	}
	
	public void setLeftPath(String leftPath) {
		this.leftPath = leftPath;
	}
	
	public String getRightPath() {
		return rightPath;
	}
	
	public void setRightPath(String rightPath) {
		this.rightPath = rightPath;
	}
	
	public String getOutputPath() {
		return outputPath;
	}
	
	public void setOutputPath(String outputPath) {
		this.outputPath = outputPath;
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

	public void setCompressionCodec(String compressionCode) {
		this.compressionCodec = compressionCode;
	}
}
