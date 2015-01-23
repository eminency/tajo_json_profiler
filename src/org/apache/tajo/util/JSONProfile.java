package org.apache.tajo.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig.Feature;
import org.codehaus.jackson.type.TypeReference;

public class JSONProfile {
	static DecimalFormat df1 = new DecimalFormat("#######################");
	static DecimalFormat df2 = new DecimalFormat("###.#####");
	static ObjectMapper om = new ObjectMapper();

	static enum RESOURCE_TYPE {
		CPU, DISK, NETWORK
	};
	
	static Map<String, RESOURCE_TYPE> stepResourceMap = new HashMap<String, RESOURCE_TYPE>();
	
	//EB list -> Exec List -> Step List(String->[Long])
	public static final TypeReference<List<Object>> TYPE = new TypeReference<List<Object>>() {};

	static {
		om.getDeserializationConfig().disable(
				DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES);
		om.getSerializationConfig().enable(Feature.INDENT_OUTPUT);
		
		//add disk and network, others are CPU
		stepResourceMap.put("InternalParquetRecordReader.page", RESOURCE_TYPE.DISK);
		stepResourceMap.put("CSVScanner.page", RESOURCE_TYPE.DISK);
		stepResourceMap.put("HashShuffleFileWriteExec.flush.nanoTime", RESOURCE_TYPE.DISK);
		stepResourceMap.put("HashShuffleFileWriteExec.next.nanoTime", RESOURCE_TYPE.DISK);
		stepResourceMap.put("RawFileScanner.read", RESOURCE_TYPE.DISK);
		stepResourceMap.put("StoreTableExec.next.nanoTime", RESOURCE_TYPE.DISK);
		stepResourceMap.put("RangeShuffleFileWriteExec.next.nanoTime",RESOURCE_TYPE.DISK);
		stepResourceMap.put("ExternalSortExec.SortScan.nanoTime", RESOURCE_TYPE.DISK);
		stepResourceMap.put("ExternalSortExec.SortWrite.nanoTime", RESOURCE_TYPE.DISK);
		stepResourceMap.put("fetch.write:", RESOURCE_TYPE.DISK);
		stepResourceMap.put("fetch", RESOURCE_TYPE.NETWORK);
	}
	
	// Check q16 (minus), q20_4(selection)
	// q20_5(range shuffle)
	// q21_1(sort), q21_2(select), 
	// q11_1
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("Usage: java <plan file> <profile file dir> <profile json files>");
			return;
		}

		File planFile = new File(args[0]);
		List<ExecutionBlock> profileData = new ArrayList<ExecutionBlock>();

		String[] jsonFiles = args[2].split(",");
		for (String eachFile: jsonFiles) {
			profileData.addAll(parseProfileData(new File(args[1] + "/" + eachFile.trim())));
		}
		
		List<String[]> planLists = new ArrayList<String[]>();		
		
		BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(planFile)));
		try {
			String line = null;
			while ((line = reader.readLine()) != null) {
				if (line.trim().isEmpty() || line.startsWith("#")) {
					continue;
				}
				planLists.add(line.split(","));
			}
		} finally {
			reader.close();
		}
		
		String[][] plans = planLists.toArray(new String[][]{});
		
		int ebIndex = 0;
		long totalTime = 0;
		ProfileContext profileContext = new ProfileContext();
		
		for (ExecutionBlock eachEb:  profileData) {
			//pre-vist for collecting scan data
			previsitForScanData(plans, ebIndex, profileContext, eachEb);
			
			// first is ebid 
			for (int i = 1; i < plans[ebIndex].length; i++) {
				String className = plans[ebIndex][i].trim();
				if (className.indexOf("_") > 0) {
					className = className.substring(0, className.indexOf("_"));
				}
				Class clazz = Class.forName("org.apache.tajo.util." + className + "ProfileUnit");
				ProfileUnit profileUnit;
				try {
					profileUnit = (ProfileUnit)clazz.newInstance();
				} catch (Exception e) {
					System.out.println("ERROR:org.apache.tajo.util." + className + "ProfileUnit");
					e.printStackTrace();
					return;
				}
				ExecData execData = eachEb.getExec(plans[ebIndex][i].trim());
				ExecData prevExec = i > 1 ? eachEb.getExec(plans[ebIndex][i - 1]) : null;
				List<Step> steps = profileUnit.profile(profileContext, execData, prevExec);
				execData.steps = steps;
			}
			
			// rate1
			long ebTotal = eachEb.execs.get(eachEb.execs.size() - 1).totalSum;
			for (int i = 1; i < plans[ebIndex].length; i++) {
				ExecData execData = eachEb.getExec(plans[ebIndex][i].trim());
				for (Step eachStep: execData.steps) {
					eachStep.rate1 = (float)eachStep.realTime/(float)ebTotal;
				}
			}
			totalTime += ebTotal;
			ebIndex++;
		}
		ebIndex = 0;
		String prevFileName = null;
		float cpuStepSum = 0;
		float diskStepSum = 0;
		float networkStepSum = 0;
		
		for (ExecutionBlock eachEb:  profileData) {
			if (prevFileName == null) {
				System.out.println("\n\n" + eachEb.fileName + "\n");	
				prevFileName = eachEb.fileName;
			} else if (!prevFileName.equals(eachEb.fileName)) {
				System.out.println("\n\n" + eachEb.fileName + "\n");	
				prevFileName = eachEb.fileName;
			}
			for (int i = 1; i < plans[ebIndex].length; i++) {
				ExecData execData = eachEb.getExec(plans[ebIndex][i].trim());
				for (Step eachStep: execData.steps) {
					eachStep.rate2 = (float)eachStep.realTime/(float)totalTime;
					String stepTypeName = eachStep.getStepTypeName();
					if (stepResourceMap.containsKey(stepTypeName)) {
						switch (stepResourceMap.get(stepTypeName)) {
							case DISK:
								diskStepSum += eachStep.rate2;
								break;
							case NETWORK:
								networkStepSum += eachStep.rate2;
								break;
							default:
								cpuStepSum += eachStep.rate2;
								break;
						}
					} else {
						cpuStepSum += eachStep.rate2;
					}
				}
				String ebId = i == 1 ? eachEb.ebId : "";
				System.out.println(execData.toString(ebId));
			}
			ebIndex++;	
			System.out.println("\n");
		}
		System.out.println("\n\n" + planFile.getName());
		System.out.println(cpuStepSum + "," + diskStepSum + "," + networkStepSum);
	}

	private static void previsitForScanData(String[][] plans, int ebIndex,
			ProfileContext profileContext, ExecutionBlock eachEb)
			throws ClassNotFoundException {
		long fileScanTotal = 0;
		long seqScanNodeTotal = 0;
		for (int i = 1; i < plans[ebIndex].length; i++) {
			String className = plans[ebIndex][i].trim();
			if (className.indexOf("_") > 0) {
				className = className.substring(0, className.indexOf("_"));
			}
			Class clazz = Class.forName("org.apache.tajo.util." + className + "ProfileUnit");
			ProfileUnit profileUnit;
			try {
				profileUnit = (ProfileUnit)clazz.newInstance();
			} catch (Exception e) {
				System.out.println("ERROR:org.apache.tajo.util." + className + "ProfileUnit");
				e.printStackTrace();
				return;
			}
			ExecData execData = eachEb.getExec(plans[ebIndex][i].trim());
			
			long value = profileUnit.preVisit(execData);
			if (profileUnit.isFileScanner()) {
				fileScanTotal += value;
			} else if (className.equals("SeqScanExec")) {
				seqScanNodeTotal += value;
			}
		}
		profileContext.setFileScanTotal(fileScanTotal);
		profileContext.setSeqScanTotal(seqScanNodeTotal);
	}
	
	private static List<ExecutionBlock> parseProfileData(File jsonFile) throws Exception {
		byte[] buf = new byte[1024 * 1024];
		FileInputStream in = new FileInputStream(jsonFile);
		int readBytes = in.read(buf);
		in.close();
		
		String jsonStr = new String(buf, 0, readBytes);
		int tagIndex = jsonStr.indexOf("<pre>");
		if (tagIndex > 0) {
			int tagEndIndex = jsonStr.indexOf("</pre>");
			jsonStr = jsonStr.substring(tagIndex + 5, tagEndIndex).trim();
		}
		
		List<Object> jsonData = om.readValue(jsonStr, TYPE);
		
		List<ExecutionBlock> profileData = new ArrayList<ExecutionBlock>();
		for (Object items: jsonData) {
			ArrayList<Object> array1 = (ArrayList)items;
			String ebId = array1.get(0).toString();
			ExecutionBlock eb = new ExecutionBlock();
			eb.ebId = ebId;
			eb.fileName = jsonFile.getName();
			profileData.add(eb);
			
			List<ExecData> execs = new ArrayList<ExecData>();
			eb.execs = execs;
			for (int i = 1; i < array1.size(); i++) {
				ArrayList<Object> array2 = (ArrayList)array1.get(i);
				List<Step> steps = new ArrayList<Step>();
				ExecData execData = new ExecData();
				execs.add(execData);
				execData.steps = steps;
				for (int j = 0; j < array2.size(); j++) {
					ArrayList<Object> array3 = (ArrayList)array2.get(j);
					String stepName = array3.get(0).toString();
					Long value = Long.parseLong(array3.get(1).toString());
					
					if (execData.name == null && stepName.split("\\.").length > 1) {
						execData.name = stepName.split("\\.")[0];
					} else {
						if (execData.name == null) {
							execData.name = "Task";
						}
					}
					
					if (stepName.endsWith("numInTuple") || stepName.endsWith("inTuples")) {
						execData.inTuples = value;
					} else if (stepName.endsWith("outTuples")) {
						execData.outTuples = value;
					} else {
						Step step = new Step();
						step.name = stepName;
						step.totalTime = value;						
						steps.add(step);
					}
				}
			}
		}
		return profileData;
	}
	

	public static class ExecutionBlock {
		String fileName;
		String ebId;
		List<ExecData> execs;
		
		public ExecData getExec(String name) {
			for (ExecData eachExec: execs) {
				if (eachExec.name.startsWith(name)) {
					return eachExec;
				}
			}
			
			return null;
		}
	}
	
	public static class ExecData {
		String name;
		long totalSum;
		long realSum;
		long inTuples;
		long outTuples;
		
		List<Step> steps;
		
		String printEbId;
		
		public Step getStep(String name) {
			for (Step eachStep: steps) {
				if (name.equals(eachStep.name)) {
					return eachStep;
				}
			}
			return null;
		}
		
		public String toString(String ebId) {
			this.printEbId = ebId;
			return toString();
		}
		
		public int getId() {
			int index1 = name.indexOf("_");
			if (index1 < 0) {
				return -100;
			}
			int id = Integer.parseInt(name.substring(index1 + 1));
			return id;
		}
		public String toString() {
			String result = "";
			float sumRate1 = 0;
			float sumRate2 = 0;
			String prefix = printEbId;
			
			String newLine = "";
			for (Step eachStep: steps) {
				result += newLine + prefix + "," + eachStep.toString();
				sumRate1 += eachStep.rate1;
				sumRate2 = eachStep.rate2;
				prefix = "";
				newLine = "\n";
			}
			result += newLine + prefix + ",SUM," + 
					df1.format(totalSum) + "," + 
					df1.format(inTuples) + "," + 
					df1.format(outTuples) + "," + 
					df1.format(realSum) + "," + 
					df2.format(sumRate1) + "," + 
					df2.format(sumRate2);
			return result;
		}
	}
	
	static class Step {
		String name;
		long totalTime;
		long realTime;
		float rate1;
		float rate2;
		
		public String toString() {
			return name + "," + df1.format(totalTime) + ",,," + df1.format(realTime) + "," + 
					df2.format(rate1) + "," + df2.format(rate2);  
		}
		
		public String getStepTypeName() {
			int index1 = name.indexOf("_");
			if (index1 < 0) {
				return name;
			}
			
			int index2 = name.indexOf(".");
			if (index2 < 0) {
				return name;
			}
			String typeName = name.substring(0, index1) + name.substring(index2);
			
			//System.out.println(">>>>>>>>" + typeName);
			return typeName;
		}
	}
}
