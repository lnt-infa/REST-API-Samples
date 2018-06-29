package com.infa.eic.icsscanner;


import java.math.BigDecimal;
import java.util.HashMap;
import java.util.HashSet;

import com.infa.products.ldm.core.rest.v2.client.api.ModelInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectInfoApi;
import com.infa.products.ldm.core.rest.v2.client.api.ObjectModificationApi;
import com.infa.products.ldm.core.rest.v2.client.invoker.ApiException;
import com.infa.products.ldm.core.rest.v2.client.models.AttributeResponse;
import com.infa.products.ldm.core.rest.v2.client.models.AttributesResponse;
import com.infa.products.ldm.core.rest.v2.client.models.FactResponse;
import com.infa.products.ldm.core.rest.v2.client.models.LinkedObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectResponse;
import com.infa.products.ldm.core.rest.v2.client.models.ObjectsResponse;

/**
 * @author gpathak
 *
 */
public final class APIUtils {
	
	public static final String TABLE_CLASSTYPE="com.infa.ldm.relational.Table";
	public static final String COL_CLASSTYPE="com.infa.ldm.relational.Column";
	public static final String DOMAIN_CLASSTYPE="com.infa.ldm.profiling.DataDomain";
	public static final String CORE_NAME="core.name";
	public static final String CORE_RESOURCE_NAME="core.resourceName";
	public static final String BGTERM="com.infa.ldm.bg.BGTerm";
	public static final String MAPPING_CLASSTYPE="com.infa.ldm.etl.pc.Mapping";
	
	public static final String DATASET_FLOW="core.DataSetDataFlow";
	
	/**
	 * Access URL of the EIC Instance
	 */
//	private static String URL="http://pslxclaire.informatica.com:9085/access/2";
	
	/**
	 * Credentials.
	 */
//	private static String USER="Administrator"; //Enter User name
//	private static String PASS="admin"; //Enter password
	
	
	
	public final static ObjectInfoApi READER = new ObjectInfoApi(); 
	public final static ObjectModificationApi WRITER = new ObjectModificationApi();

	public final static ModelInfoApi MODEL_READER = new ModelInfoApi(); 

		
	public final static void setupOnce(String URL, String USER, String PASS) {
		READER.getApiClient().setUsername(USER);
		READER.getApiClient().setPassword(PASS);
		READER.getApiClient().setBasePath(URL);		
	
		WRITER.getApiClient().setUsername(USER);
		WRITER.getApiClient().setPassword(PASS);
		WRITER.getApiClient().setBasePath(URL);		
		MODEL_READER.getApiClient().setUsername(USER);
		MODEL_READER.getApiClient().setPassword(PASS);
		MODEL_READER.getApiClient().setBasePath(URL);		
	}
	
	public static final String getValue(ObjectResponse obj, String name) {
		for(FactResponse fact:obj.getFacts()) {
			if(name.equals(fact.getAttributeId())) {
				return fact.getValue();
			}
		}
		return null;
	}
	
	  public static String getCustomAttributeID(String customAttributeName) throws Exception {
			int total=1000;
			int offset=0;
			final int pageSize=300;
			
			String customAttributeId = new String();
			boolean dup = false;
			
			while (offset<total) {
				try {			
					AttributesResponse response=APIUtils.MODEL_READER.catalogModelsAttributesGet(null, null, BigDecimal.valueOf(offset), BigDecimal.valueOf(pageSize));
					total=response.getMetadata().getTotalCount().intValue();
					offset+=pageSize;
					
					for(AttributeResponse ar: response.getItems()) {					
						if(ar.getName().equals(customAttributeName)) {
							if (customAttributeId != null && ! customAttributeId.equals("")) dup = true;
							customAttributeId=ar.getId();					
						}
					}
				} catch (ApiException e) {
					e.printStackTrace();
				}
			}
			
			if (customAttributeId.equals("")) { 			
				throw new Exception("Custom Attribute ID not found");
			} else if (dup) {
				throw new Exception("Duplicate Attribute ID found");
	  		} else {
				return customAttributeId;
			}
	  }
		

}
