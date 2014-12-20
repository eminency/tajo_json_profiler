package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class SeqScanExecProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		float rate = (float)next.totalTime/(float)profileContext.getSeqScanTotal();

		long scanTimeForThisExec = (long)((float)profileContext.getFileScanTotal() * rate);
		
		Step binaryEval = exec.getStep(exec.name + ".BinaryEval.nanoTime");
		if (binaryEval != null) {
			binaryEval.realTime = binaryEval.totalTime;
		}
		
		Step notEval = exec.getStep(exec.name + ".NotEval.nanoTime");
		if (notEval != null) {
			notEval.realTime = notEval.totalTime;
		}
		Step project = exec.getStep(exec.name + ".project.nanoTime");
		project.realTime = project.totalTime;
		
		long  evalRealTime = 0;
		next.realTime = next.totalTime - scanTimeForThisExec - project.realTime;
		if (binaryEval != null) {
			next.realTime = next.realTime - binaryEval.realTime;
			evalRealTime += binaryEval.realTime;
		} 
		if (notEval != null) {
			next.realTime = next.realTime - notEval.realTime;
			evalRealTime += notEval.realTime;
		} 
		
		List<Step> steps = new ArrayList<Step>();
		if (binaryEval != null) {
			steps.add(binaryEval);
		}
		if (notEval != null) {
			steps.add(notEval);
		}
		steps.add(project);
		steps.add(next);
		
		exec.totalSum = next.totalTime;
		exec.realSum = next.realTime + project.realTime + evalRealTime;
		return steps;
	}
	
	public long preVisit(ExecData exec) {
		Step next = exec.getStep(exec.name + ".next.nanoTime");
		return next.totalTime;
	}
}
