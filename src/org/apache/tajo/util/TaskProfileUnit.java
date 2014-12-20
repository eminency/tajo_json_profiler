package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class TaskProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		Step fetchWrite = exec.getStep("fetch.write");
		fetchWrite.realTime = fetchWrite.totalTime;
		
		Step fetch = exec.getStep("fetch");
		if (fetch.totalTime < 0) {
			fetch.totalTime = 0;
		}
		fetch.realTime = fetch.totalTime - fetchWrite.realTime;
		
		Step total = exec.getStep("total");
		total.realTime = total.totalTime - prevExec.totalSum - fetch.totalTime;
		
		List<Step> steps = new ArrayList<Step>();
		
		steps.add(fetchWrite);
		steps.add(fetch);
		steps.add(total);
		
		exec.totalSum = total.totalTime;
		exec.realSum = total.realTime + fetch.realTime + fetchWrite.realTime;
		return steps;
	}

}
