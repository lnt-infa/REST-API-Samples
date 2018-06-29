package com.infa.eic.icsscanner;

import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.*;
import com.opencsv.CSVReader;


import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.models.FactRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectIdRequest;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;
import com.infa.products.ldm.core.rest.v2.client.utils.ObjectAdapter;


public class UpdateTaskCustomAttrName {	
	
	public static void main(String[] args) {
		Options options = new Options();
		
        Option inputOpt = new Option("i", "input", true, "input file path");
        inputOpt.setRequired(true);
        options.addOption(inputOpt);

        Option userOpt = new Option("u", "user", true, "Username");
        userOpt.setRequired(true);
        options.addOption(userOpt);

        Option passOpt = new Option("p", "pass", true, "Password");
        passOpt.setRequired(true);
        options.addOption(passOpt);        
        
        Option urlOpt = new Option("l", "url", true, "EDC URL");
        urlOpt.setRequired(true);
        options.addOption(urlOpt); 
        
        Option resOpt = new Option("r", "resource", true, "EDC Resource Name to update");
        resOpt.setRequired(true);
        options.addOption(resOpt);
        
        Option caOpt = new Option("a", "attribute", true, "Custom Attribute Name");
        caOpt.setRequired(true);
        options.addOption(caOpt);
        
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("UpdateTaskCustomAttrName", options);

            System.exit(1);
            return;
        }

        String inputFilePath = cmd.getOptionValue("input");
        String user = cmd.getOptionValue("user");
        String pass = cmd.getOptionValue("pass");
        String url = cmd.getOptionValue("url");
        String resourceName = cmd.getOptionValue("resource");
        String attrName = cmd.getOptionValue("attribute");
        
        UpdateTaskCustomAttrName b=new UpdateTaskCustomAttrName();
        
		try {
			APIUtils.setupOnce(url+"/access/2", user, pass);
						
	        String query = APIUtils.CORE_RESOURCE_NAME+":"+resourceName +" AND core.allclassTypes:\""+ APIUtils.MAPPING_CLASSTYPE +"\"";
	        String attrId= APIUtils.getCustomAttributeID(attrName);
	        
	        HashMap<String,String> valuesMap = new HashMap<String,String>();
	        CSVReader reader = new CSVReader(new FileReader(inputFilePath));
	        String[] rec = null;
	        while ((rec = reader.readNext()) != null) {
	        	valuesMap.put(rec[0], rec[1]);
	        }
	        reader.close();
	        
	        b.bulkUpdater(attrId,query, valuesMap);						
	        
		}catch(Exception e) {
			e.printStackTrace();
		}
        		
	}
	
	public void bulkUpdater(String attrId,String query, HashMap<String,String> valuesMap) throws Exception  {
		int total=1000;
		int offset=0;
		final int pageSize=20;		
		
		while (offset<total) {
			ObjectsResponse response=APIUtils.READER.catalogDataObjectsGet(query, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize), false);
			
			total=response.getMetadata().getTotalCount().intValue();
			offset+=pageSize;
			
			
			for(ObjectResponse or: response.getItems()) {
				
				ObjectIdRequest request=ObjectAdapter.INSTANCE.copyIntoObjectIdRequest(or);

				String tgtVal=APIUtils.getValue(or,attrId);
				// only update value if targetValue is not set of if override flag set to true
				
				Pattern p = Pattern.compile("([0-9A-Z]{20})");
				Matcher m = p.matcher(or.getId());
				String id = "";
				if(m.find()) {
				  id = m.group(0);
				} 
				
				
				if(valuesMap.get(id) != null) {
					String val=valuesMap.get(id);
					//request.getFacts().remove(new FactRequest().attributeId(tgtCustomAttributeID).value(srcVal));
					request.addFactsItem(new FactRequest().attributeId(attrId).value(val));		
					
														
					String ifMatch;
					try {
						ifMatch = APIUtils.READER.catalogDataObjectsIdGetWithHttpInfo(or.getId()).getHeaders().get("ETag").get(0);
					
						ObjectResponse newor=APIUtils.WRITER.catalogDataObjectsIdPut(or.getId(), request, ifMatch);
						System.out.println(or.getId()+":"+val);
					} catch (ApiException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} 
				
			}
			if(offset >= total) System.out.println(total+"/"+total);
			else System.out.println(offset+"/"+total);
		}		
		
	}
}
