package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class HashAggregateExecProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step compute = exec.getStep(exec.name + ".compute.nanoTime");
		compute.realTime = compute.totalTime - prevExec.totalSum;
		
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		next.realTime = next.totalTime - compute.totalTime;
		
		List<Step> steps = new ArrayList<Step>();
		steps.add(compute);
		steps.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime + compute.realTime;
		
		return steps;
	}

}
