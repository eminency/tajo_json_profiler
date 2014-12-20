package org.apache.tajo.util;

public class ProfileContext {
	long fileScanTotal;
	long seqScanTotal;
	
	public long getFileScanTotal() {
		return fileScanTotal;
	}
	public void setFileScanTotal(long fileScanTotal) {
		this.fileScanTotal = fileScanTotal;
	}
	public long getSeqScanTotal() {
		return seqScanTotal;
	}
	public void setSeqScanTotal(long seqScanTotal) {
		this.seqScanTotal = seqScanTotal;
	}
	
	
}
