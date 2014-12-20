package org.apache.tajo.util;

import java.util.List;

import org.apache.tajo.util.JSONProfile.ExecData;
import org.apache.tajo.util.JSONProfile.Step;

public abstract class ProfileUnit {
	public abstract List<Step> profile(ProfileContext profileContext, ExecData exec, ExecData prevExec);
	
	public long preVisit(ExecData exec) {
		return 0;
	}
	
	public boolean isFileScanner() {
		return false;
	}
}
