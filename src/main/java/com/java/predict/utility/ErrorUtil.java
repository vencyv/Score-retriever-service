package com.java.predict.utility;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.stereotype.Component;

/**
 * @author RuchitM
 * Utility Class to return error response 
 */
@Component
public class ErrorUtil {

	private static final Logger logger = LogManager.getLogger(ErrorUtil.class);

	/**
	 * This method will return the error response
	 * @param exceptionMsg
	 * @param errorCode
	 * @return
	 */
	public String getFlowInvokerError(String exceptionMsg,String errorCode)
	{
		JSONObject jsonObject = new JSONObject();
		try
		{
			jsonObject.put("status", "error");
			jsonObject.put("code", errorCode);
			jsonObject.put("response", exceptionMsg);
		}
		catch(JSONException e)
		{
		//throw 	
		}
		return jsonObject.toString();
	}
}
