package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class InternalParquetRecordReaderProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step page = exec.getStep("InternalParquetRecordReader.page");
		page.realTime = page.totalTime;
			
		Step next = exec.getStep("InternalParquetRecordReader.next");
		next.realTime = next.totalTime - page.realTime;
		
		List<Step> computedStep = new ArrayList<Step>();
		computedStep.add(page);
		computedStep.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime;
		
		return computedStep;
	}	
	public long preVisit(ExecData exec) {
		Step next = exec.getStep("InternalParquetRecordReader.next");
		return next.totalTime;
	}
	
	public boolean isFileScanner() {
		return true;
	}
}
