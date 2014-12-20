package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class HashShuffleFileWriteExecProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		next.realTime = next.totalTime - prevExec.totalSum;
		
		Step flush = exec.getStep(exec.name + ".flush.nanoTime");
		flush.realTime = flush.totalTime;
		
		List<Step> steps = new ArrayList<Step>();
		steps.add(next);
		steps.add(flush);
		
		exec.totalSum = flush.totalTime + next.totalTime;
		exec.realSum = flush.realTime + next.realTime;
		
		return steps;
	}

}
