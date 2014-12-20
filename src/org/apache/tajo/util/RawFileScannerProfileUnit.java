package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class RawFileScannerProfileUnit extends ProfileUnit {
	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step read = exec.getStep("RawFileScanner.read");
		read.realTime = read.totalTime;
		
		Step makeTuple = exec.getStep("RawFileScanner.makeTuple");
		makeTuple.realTime = makeTuple.totalTime;
		
		Step next = exec.getStep("RawFileScanner.next");
		next.realTime = next.totalTime - read.realTime - makeTuple.realTime;
		
		List<Step> steps = new ArrayList<Step>();
		steps.add(read);
		steps.add(makeTuple);
		steps.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime + makeTuple.realTime + read.realTime;
		return steps;
	}
	
	public long preVisit(ExecData exec) {
		Step next = exec.getStep("RawFileScanner.next");
		return next.totalTime;
	}
	
	public boolean isFileScanner() {
		return true;
	}
}
