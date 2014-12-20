package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class DistinctGroupbySortAggregationExecProfileUnit extends ProfileUnit {
	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		next.realTime = next.totalTime - prevExec.totalSum;
		
		List<Step> steps = new ArrayList<Step>();
		steps.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime;
		return steps;
	}
}
