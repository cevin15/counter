package cn.com.counter.container;

public class Counts {
	private long bufferCounts;

	private long dbCounts;

	public long getBufferCounts() {
		return bufferCounts;
	}

	public void setBufferCounts( long bufferCounts ) {
		this.bufferCounts = bufferCounts;
	}

	public long getDbCounts() {
		return dbCounts;
	}

	public void setDbCounts( long dbCounts ) {
		this.dbCounts = dbCounts;
	}

	@Override
	public String toString() {
		return super.toString() + "|dbCounts-->" + dbCounts + "|bufferCounts-->" + bufferCounts;
	}
}