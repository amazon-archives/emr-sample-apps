package logprocessor.enums;

public enum ReportNames {
	RequestCount("request-count"),
	RequestCountByHttpResponse("request-count-by-httpresponse"),
	BytesTransferred("bytes-transferred");
	
	private String name;

	ReportNames(String r) {
		this.name = r;
	}
	
	@Override
	public String toString() {
		return name;
	}
}
