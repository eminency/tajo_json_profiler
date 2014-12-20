package org.apache.tajo.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public class ExternalSortExecProfileUnit extends ProfileUnit {

	@Override
	public List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec) {
		List<Step> steps = new ArrayList<Step>();
		
		int execId = exec.getId();
		if (execId < 0) {
			//SortAggregation
			Step sortWrite = exec.getStep(exec.name + ".SortWrite.nanoTime");
			sortWrite.realTime = sortWrite.totalTime;
			Step sortScan = exec.getStep(exec.name + ".SortScan.nanoTime");
			sortScan.realTime = sortScan.totalTime;
			
			Step sort = exec.getStep(exec.name + ".Sort.nanoTime");
			sort.realTime = sort.totalTime - sortScan.realTime - sortWrite.realTime - prevExec.totalSum;
			
			Step next = exec.getStep(exec.name + ".next.nanoTime");
			next.realTime = next.totalTime - sort.totalTime;

			steps.add(sortScan);
			steps.add(sortWrite);
			steps.add(sort);
			steps.add(next);
			
			exec.totalSum = next.totalTime;
			exec.realSum = next.realTime + sort.realTime + sortWrite.realTime + sortScan.realTime;
			return steps;
		} else {
			//Order by
			Step next = exec.getStep(exec.name + ".next.nanoTime");
			next.realTime = next.totalTime - prevExec.totalSum;
			exec.totalSum = next.totalTime;
			exec.realSum = next.realTime;
			steps.add(next);
			
			return steps;
		}
		
	}

}
