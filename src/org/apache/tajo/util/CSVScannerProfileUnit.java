package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class CSVScannerProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step page = exec.getStep("CSVScanner.page");
		page.realTime = page.totalTime;
		
		Step makeTuple = exec.getStep("CSVScanner.makeTuple");
		makeTuple.realTime = makeTuple.totalTime;
		
		Step next = exec.getStep("CSVScanner.next");
		next.realTime = next.totalTime - page.realTime - makeTuple.realTime;
		
		List<Step> computedStep = new ArrayList<Step>();
		computedStep.add(page);
		computedStep.add(makeTuple);
		computedStep.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime + makeTuple.realTime + page.realTime;
		
		return computedStep;
	}	
	public long preVisit(ExecData exec) {
		Step next = exec.getStep("CSVScanner.next");
		return next.totalTime;
	}
	
	public boolean isFileScanner() {
		return true;
	}
}
