package hancock.data.compare.cli;

public class CompareOptions {
	private String leftPath;
	private String rightPath;
	private String outputPath;
	
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
}
