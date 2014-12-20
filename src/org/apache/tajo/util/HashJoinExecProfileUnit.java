package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class HashJoinExecProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		long scanTotal = profileContext.getSeqScanTotal() == 0 ? profileContext.getFileScanTotal() : profileContext.getSeqScanTotal();
		
		Step right = exec.getStep(exec.name + ".nanoTimeLoadRight");
		Step left = exec.getStep(exec.name + ".nanoTimeLeftNext");
		
		long sum = right.totalTime + left.totalTime;
		right.realTime = prevExec != null ? (sum - scanTotal)/2 : sum/2;
		left.realTime = prevExec != null ? (sum - scanTotal)/2 : sum/2;
		
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		next.realTime = next.totalTime - right.realTime - left.realTime - scanTotal;
		
		List<Step> steps = new ArrayList<Step>();
		steps.add(right);
		steps.add(left);
		steps.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime + left.realTime + right.realTime;
		
		return steps;
	}

}
