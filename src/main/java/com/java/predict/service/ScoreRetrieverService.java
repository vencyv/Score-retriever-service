package com.java.predict.service;

/**
 * @author RuchitM
 * Poll message from data aggregator queue
 * Make the contract to call the model 
 * call the model according to message coming from queue
 * update data in epic coming from model
 *  
 */
public interface ScoreRetrieverService {

	public void pollMessageFromQueue();
	public String getSecretFromKeyVault(String requestParam)throws Exception;
	public void appendLog(String methodName, String type, String message);
	public void pushLogIntoLoggingservice();
	public String getBatchDateTime();
	public void updateDataIntoStorageBlob(String scoreReference, String score) throws Exception;
}
