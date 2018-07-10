package com.java.predict.service;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import com.java.predict.utility.ErrorUtil;
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.windowsazure.Configuration;
import com.microsoft.windowsazure.exception.ServiceException;
import com.microsoft.windowsazure.services.servicebus.ServiceBusConfiguration;
import com.microsoft.windowsazure.services.servicebus.ServiceBusContract;
import com.microsoft.windowsazure.services.servicebus.ServiceBusService;
import com.microsoft.windowsazure.services.servicebus.implementation.BrokerProperties;
import com.microsoft.windowsazure.services.servicebus.models.BrokeredMessage;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMessageOptions;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveMode;
import com.microsoft.windowsazure.services.servicebus.models.ReceiveQueueMessageResult;

/**
 * @author RuchitM
 * poll messages from Data Aggregator queue
 */
@Service
@PropertySource(value={"classpath:appConstant.properties"})
public class ScoreRetrieverServiceImpl implements ScoreRetrieverService
{ 	
	@Value("${error.predictScoreError}")
	private String predictScoreError;

	@Value("${error.fetchDataFromStorageBlobError}")
	private String fetchDataFromStorageBlobError;

	@Value("${error.updateDataIntoStorageBlobError}")
	private String updateDataIntoStorageBlobError;

	@Value("${error.epicError}")
	private String epicError;

	@Value("${error.keyVaultError}")
	private String keyVaultError;

	@Value("${error.pollingMsgError}")
	private String pollingMsgError;

	private String SERVICE_BUS_NAMESPACE = System.getenv("SERVICE_BUS_NAMESPACE");
	private String SERVICE_BUS_SAS_KEY_NAME = System.getenv("SERVICE_BUS_SAS_KEY_NAME");
	private String SERVICE_BUS_QUEUE_SHARED_ACCESS_KEY_NAME  = System.getenv("SERVICE_BUS_QUEUE_SHARED_ACCESS_KEY_NAME");
	private String SERVICE_BUS_ROOT_URI = System.getenv("SERVICE_BUS_SERVICE_BUS_ROOT_URI");
	private String SERVICE_BUS_SCORE_RETRIEVER_QUEUE_NAME = System.getenv("SERVICE_BUS_SCORE_RETRIEVER_QUEUE_NAME");
	private String LOGGING_URL = System.getenv("LOGGING_URL");
	private String EPIC_URL = System.getenv("EPIC_URL");
	private String STORAGE_ACCOUNT_ACCESS_KEY_NAME = System.getenv("STORAGE_ACCOUNT_ACCESS_KEY_NAME");
	private String BLOB_CONTAINER_NAME = System.getenv("BLOB_CONTAINER_NAME");
	private String TIMEZONE = System.getenv("TIMEZONE");
	private String SCORE_KEY = System.getenv("SCORE_KEY");
	private String MODEL_URL = System.getenv("MODEL_URL");
	private String KEYVAULT_SERVICE_ENDPOINT = System.getenv("KEYVAULT_SERVICE_ENDPOINT");
	private String ISDEBUG = System.getenv("ISDEBUG");
	


	@Autowired
	private ErrorUtil errorUtil;

	@Autowired
	private RetryTemplate retryTemplate;

	private static final Logger logger = LogManager.getLogger(ScoreRetrieverServiceImpl.class);

	JSONArray logArray = new JSONArray();

	/* 
	 * Method to poll messages from Data Aggregator queue
	 */
	@Override
	public void pollMessageFromQueue() {

		logger.info("Enter pollMessageFromQueue:");
		try
		{
			SERVICE_BUS_QUEUE_SHARED_ACCESS_KEY_NAME = getSecretFromKeyVault(SERVICE_BUS_QUEUE_SHARED_ACCESS_KEY_NAME);
			Configuration config = ServiceBusConfiguration.configureWithSASAuthentication(SERVICE_BUS_NAMESPACE, SERVICE_BUS_SAS_KEY_NAME, SERVICE_BUS_QUEUE_SHARED_ACCESS_KEY_NAME, SERVICE_BUS_ROOT_URI);
			ServiceBusContract serviceBusContract = ServiceBusService.create(config);
			ReceiveMessageOptions receiveMessageOptions = ReceiveMessageOptions.DEFAULT;
			receiveMessageOptions.setReceiveMode(ReceiveMode.PEEK_LOCK);
			receiveMessageOptions.setTimeout(30); 
			while(true)
			{
				ReceiveQueueMessageResult receiveQueueMessageResult = serviceBusContract.receiveQueueMessage(SERVICE_BUS_SCORE_RETRIEVER_QUEUE_NAME, receiveMessageOptions);
				BrokeredMessage brokeredMessage = receiveQueueMessageResult.getValue();
				if(brokeredMessage != null )
				{
					BrokerProperties brokerProperties = brokeredMessage.getBrokerProperties();
					String messageID = brokerProperties.getMessageId();
					/*appendLog("pollMessageFromQueue", "Info", "MessageID: "+messageID);
					pushLogIntoLoggingservice();*/
					String lockToken = brokerProperties.getLockToken();			
					try
					{		
						// TODO: 
						byte[] byteArr = new byte[2000];
						StringBuilder queueMsg = new StringBuilder();
						int numRead = brokeredMessage.getBody().read(byteArr);
						while (-1 != numRead)
						{
							String tempQueueMsg = new String(byteArr);
							tempQueueMsg = tempQueueMsg.trim();
							queueMsg = queueMsg.append(tempQueueMsg);
							numRead = brokeredMessage.getBody().read(byteArr);
						}
						// TODO: first check whether empty or not
						int i = queueMsg.toString().indexOf("{");
						ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
						exec.scheduleAtFixedRate(new Runnable() {
						  @Override
						  public void run() {
								try {
									logger.info("Renewing lock on message for 25sec: "+messageID);
									serviceBusContract.renewQueueLock(SERVICE_BUS_SCORE_RETRIEVER_QUEUE_NAME, messageID, lockToken);
								} catch (ServiceException e) {
									String errorResponse = errorUtil.getFlowInvokerError(
											"Error in polling message from service bus queue : " + e.getErrorMessage(),
											pollingMsgError);
									appendLog("pollMessageFromQueue", "Exception", errorResponse);
									pushLogIntoLoggingservice();
									System.exit(0);
								}		
						  }
						}, 0, 25, TimeUnit.SECONDS);
						predictScore(queueMsg.toString().substring(i), messageID);
						logger.info("Deleting message: "+ messageID);
						serviceBusContract.deleteMessage(brokeredMessage);
						exec.shutdown();
					}
					catch (IOException ioException) {
						String errorResponse = errorUtil.getFlowInvokerError("Error in polling message from service bus queue: "+ioException.getMessage(), pollingMsgError);
						appendLog("pollMessageFromQueue", "Exception", errorResponse);
						pushLogIntoLoggingservice();
						serviceBusContract.unlockMessage(brokeredMessage);
						logger.info("Error response:"+messageID);
						break;
					}	
					catch(Exception exception)
					{
						String errorResponse = errorUtil.getFlowInvokerError("Error in polling message from service bus queue:  "+ exception.getMessage(), pollingMsgError);
						appendLog("pollMessageFromQueue", "Exception", errorResponse);
						pushLogIntoLoggingservice();
						logger.info("Unlocking message: "+errorResponse);
						serviceBusContract.unlockMessage(brokeredMessage);
						break;
					}				
				}  
				else  
				{
					// TODO: 
					logger.info("Finishing up - no more messages.");
					System.exit(0);
				}		
			}
		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in polling message from service bus queue:"+exception.getMessage(), pollingMsgError);
			appendLog("pollMessageFromQueue", "Exception", errorResponse);
			pushLogIntoLoggingservice();
			exception.printStackTrace();
			System.exit(0);
		}
		logger.info("Exit pollMessageFromQueue:");
		System.exit(0);
	}

	/* 
	 * Method to call model service according to given model name
	 * and it will return the predicted risk score 
	 */
	public void predictScore(String queueMsg, String messageID) throws Exception
	{		
		logger.info("Enter predictScore:");
		try
		{
			logger.info("message from queue: "+queueMsg);
			JSONObject queueMsgJsonObject = new JSONObject(queueMsg);
			String modelName = queueMsgJsonObject.getString("ModelName");
			String modelVersion = queueMsgJsonObject.getString("ModelVersion");
			String transformedReference = queueMsgJsonObject.getString("TransformedReference");
			String scoreReference = queueMsgJsonObject.getString("ScoreReference");	
			JSONArray apiJsonArray = queueMsgJsonObject.getJSONArray("API");

			//String transformedJson = "[{\"PatientID\":12345,\"Patient.Age\":24,\"ProblemList\":[{\"Problem.DiagnosisIDs\":{\"Problem.DiagnosisIDs.ID\":1,\"Problem.DiagnosisIDs.IDType\":\"Internal\"},\"Problem.ProblemName\":\"ABRASION\",\"Problem.DateNoted\":\"2009-01-01T01:00:00\",\"Problem.DateEntered\":\"2009-01-01T01:00:00\",\"Problem.Class\":\"Acute\"}],\"ResultComponents\":[{\"Component.ComponentID\":1,\"Component.Abnormality.Title\":\"Abnormal\",\"Component.PrioritizedDate\":\"3/23/2015\",\"Component.PrioritizedTime\":\"9:00 AM\",\"Component.ReferenceRange\":\"4.7-6.1\",\"Component.Status.Title\":\"Final\",\"Component.Units\":\"cells/mcL\",\"Component.Value\":\"4.01\"}],\"MedicationOrders\":[{\"MedicationOrder.DispenseQuantity\":5,\"MedicationOrder.DispenseQuantityUnit\":\"tablets\",\"MedicationOrder.Dose\":100,\"MedicationOrder.DoseUnit.Number\":1,\"MedicationOrder.DoseUnit.Title\":\"mL\",\"MedicationOrder.DoseAdminDuration\":1,\"MedicationOrder.DoseAdminDurationUnit.Number\":1,\"MedicationOrder.DoseAdminDurationUnit.Title\":\"Hours\",\"MedicationOrder.EndDateTime\":\"2009-01-01T00:00:00\",\"MedicationOrder.Frequency.DisplayName\":\"Continuous\",\"MedicationOrder.Name\":\"Dextrose 5 % solution 100 mL\",\"MedicationOrder.Route.Name\":\"Intravenous\",\"MedicationOrder.StartDateTime\":\"2009-01-01T01:00:00\"}],\"Contacts\":[{\"Contact.ID\":54321,\"Contact.DateTime\":\"2009-01-01T00:00:00\",\"Contact.DepartmentID\":1,\"Contact.DepartmentName\":\"ED\",\"FlowsheetRows\":[{\"FlowsheetRow.Name\":\"Glasgow Coma Scale Score\",\"FlowsheetRow.FlowsheetRowID\":1,\"FlowsheetRow.RawValue\":3,\"FlowsheetRow.Instant\":\"2009-01-01T01:00:00\"}]}]}]";
			// call to fecth transformed json from storage blob
			String transformedJson = fetchDataFromStorageBlob(transformedReference);
			logger.info("transformedJson: "+transformedJson);
			//call to model service for the given model url 
			String score = callModelService(transformedJson);
			if(score !=null && !score.trim().isEmpty())
			{
				//call to update score into storage blob	
				updateDataIntoStorageBlob(scoreReference, score);
				//call to update score into epic
				updateDataIntoEpic(apiJsonArray, score);
			}
			else
			{
				String errorResponse = errorUtil.getFlowInvokerError("Error in predictScore(): "+"Predicted score is empty or null for queue message "+messageID, predictScoreError);
				appendLog("predictScore", "Exception", errorResponse);
				pushLogIntoLoggingservice();
			}
		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in predictScore(): "+exception.getMessage(), predictScoreError);
			appendLog("callModelService", "Exception", errorResponse);
			logger.info("errorReponse: "+errorResponse);
			throw new Exception(exception);
		}
		logger.info("Exit predictScore:");		
	}	

	public String callModelService(String transformedJson) throws Exception
	{
		logger.info("Enter callModelService:");
		String response = "";
		try
		{
			RestTemplate restTemplate = new RestTemplate();
			/*HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<String> entity = new HttpEntity<String>(transformedJson, headers);*/
			//response = restTemplate.postForObject(MODEL_URL, entity, String.class);		
			response = restTemplate.getForObject(MODEL_URL, String.class);
			JSONObject jsonObject = new JSONObject(response);
			response =  jsonObject.getString(SCORE_KEY);
		}
		catch(Exception exception)
		{
			throw new Exception(exception);
		}
		logger.info("Exit callModelService:");
		return response;
	}

	/* 
	 * Method to fetch data from storage Blob 
	 */
	public String fetchDataFromStorageBlob(String transformedReference) throws Exception
	{
		logger.info("Enter fetchDataFromStorageBlob:");
		/*final String storageConnectionString = "DefaultEndpointsProtocol=https;" + 
				"AccountName="+STORAGE_ACCOUNT_NAME+";" + 
				"AccountKey="+STORAGE_ACCOUNT_ACCESS_KEY_NAME+";" +"EndpointSuffix=core.windows.net";
*/
		final String storageConnectionString = getSecretFromKeyVault(STORAGE_ACCOUNT_ACCESS_KEY_NAME);
		String transformedJson = "";
		try
		{
			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(BLOB_CONTAINER_NAME);
			CloudBlockBlob cloudBlockBlob = cloudBlobContainer.getBlockBlobReference("Patients/E3278/2018-06-21T04:25:00-0500/Patient-Records/Transformed/Trauma");
			transformedJson = cloudBlockBlob.downloadText();		
			logger.info("Blob transformedJson:"+transformedJson);
		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in fetchDataFromStorageBlob(): "+exception.getMessage(), fetchDataFromStorageBlobError);
			appendLog("fetchDataFromStorageBlob", "Exception", errorResponse);
			throw new Exception(exception);
		}
		logger.info("Exit fetchDataFromStorageBlob:");
		return transformedJson;
	}

	/* 
	 * Method to update score into storage Blob
	 */
	@Override
	public void updateDataIntoStorageBlob(String scoreReference, String score) throws Exception
	{
		logger.info("Enter updateDataIntoStorageBlob:");
		final String storageConnectionString = getSecretFromKeyVault(STORAGE_ACCOUNT_ACCESS_KEY_NAME);
		/*"DefaultEndpointsProtocol=https;" +"AccountName="+STORAGE_ACCOUNT_NAME+";" +
				"AccountKey="+STORAGE_ACCOUNT_KEY;*/
		/*final String storageConnectionString = "DefaultEndpointsProtocol=https;" + 
				"AccountName="+STORAGE_ACCOUNT_NAME+";" + 
				"AccountKey="+STORAGE_ACCOUNT_ACCESS_KEY_NAME+";" +"EndpointSuffix=core.windows.net";
		*/
		try
		{
			logger.info("Blob uploadText:"+score);
			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.parse(storageConnectionString);
			CloudBlobClient cloudBlobClient = cloudStorageAccount.createCloudBlobClient();
			CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(BLOB_CONTAINER_NAME);
			CloudBlockBlob cloudBlockBlob = cloudBlobContainer.getBlockBlobReference(scoreReference);
			cloudBlockBlob.uploadText(score);		
		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in updateDataIntoStorageBlob(): "+exception.getMessage(), updateDataIntoStorageBlobError);
			appendLog("updateDataIntoStorageBlob", "Exception", errorResponse);
			throw new Exception(exception);
		}
		logger.info("Exit updateDataIntoStorageBlob:");
	}

	/* 
	 * Method to update data into epic web service
	 * Calling api name will come from azure service bus queue messages
	 */
	public void updateDataIntoEpic(JSONArray apiJsonArray, String score)throws Exception
	{
		logger.info("Enter updateDataIntoEpic:");
		String response = "";
		try
		{
			int apiJsonArrayLength = apiJsonArray.length();
			for(int i=0; i<apiJsonArrayLength; i++)
			{
				JSONObject apiJsonObject = apiJsonArray.getJSONObject(i);
				Iterator iterator = apiJsonObject.keys();
				while(iterator.hasNext())
				{
					JSONArray jsonArray = apiJsonObject.getJSONArray(iterator.next().toString());
					int jsonArrayLength = jsonArray.length();
					for(int j=0; j<jsonArrayLength; j++)
					{
						JSONArray paramJsonArray = new JSONArray();
						paramJsonArray.put(jsonArray.getJSONObject(i));
						JSONObject jsonObject = new JSONObject();
						jsonObject.put("API", paramJsonArray);
						String apiJson = jsonObject.toString();
						apiJson = apiJson.replaceAll("#SCORE#", score);
						RestTemplate restTemplate = new RestTemplate();
						HttpHeaders headers = new HttpHeaders();
						headers.setContentType(MediaType.APPLICATION_JSON);
						HttpEntity<String> entity = new HttpEntity<String>(apiJson, headers);
						response = restTemplate.postForObject(EPIC_URL, entity, String.class);
						if(!Boolean.parseBoolean(ISDEBUG))
						{
							JSONObject epicResponseJson = new JSONObject(response);
							if("success".equals(epicResponseJson.getString("status")))
							{
								response = epicResponseJson.getString("response");		
							}
							else
							{
								String errorResponse = errorUtil.getFlowInvokerError("Error in updateDataIntoEpic: "+epicResponseJson.getString("response"), epicError);
								appendLog("updateDataIntoEpic", "Error", errorResponse);
								throw new Exception();
							}
						}
					}
				}
			}

		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in updateDataIntoEpic: "+exception.getMessage(), epicError);
			appendLog("updateDataIntoEpic", "Error", errorResponse);
			throw new Exception(exception);
		}
		logger.info("Exit updateDataIntoEpic:");
	}

	/* 
	 * Method to get secret from azure key vault
	 */
	@Override
	public String getSecretFromKeyVault(String requestParam) throws Exception
	{
		logger.info("Enter getSecretFromKeyVault:");
		String response = "";
		try
		{
			logger.info("key vault url: "+KEYVAULT_SERVICE_ENDPOINT+requestParam);
			RestTemplate restTemplate = new RestTemplate();
			response = restTemplate.getForObject(KEYVAULT_SERVICE_ENDPOINT+requestParam, String.class);
			JSONObject jsonObject = new JSONObject(response);
			String status = jsonObject.getString("status");
			if("success".equals(status))
			{
				response = jsonObject.getString("response");
			}
			else
			{
				String errorResponse = errorUtil.getFlowInvokerError("Error in getSecretFromKeyVault: "+response, keyVaultError);
				appendLog("getSecretFromKeyVault", "Exception", errorResponse);
				throw new Exception();
			}
		}
		catch(Exception exception)
		{
			String errorResponse = errorUtil.getFlowInvokerError("Error in  getSecretFromKeyVault: "+response, keyVaultError);
			appendLog("getSecretFromKeyVault", "Exception", errorResponse);
			throw new Exception(exception);
		}
		logger.info("Exit getSecretFromKeyVault:");
		return response;
	}

	/* 
	 * Method to append all logs
	 * @Param methodName, type, message
	 */
	@Override
	public void appendLog(String methodName, String type, String message)
	{
		logger.info("Enter appendLog:");
		JSONObject logJsonObject = new JSONObject();
		try {
			logJsonObject.put("Microservice", "Score-retriever-service");
			logJsonObject.put("Class", "ScoreRetrieverServiceImpl");
			logJsonObject.put("Method", methodName);
			logJsonObject.put("Status", type);
			logJsonObject.put("Message", new JSONObject(message));
			logJsonObject.put("DateTime", getBatchDateTime());
			logArray.put(logJsonObject);
			logger.info("Exit appendLog :"+ logJsonObject.toString());
		} catch (JSONException e) {
			// TODO:
			logger.info("Exception in appendLog: "+ e.getMessage());
		}
	}

	/* 
	 * Method to push log into Logging Service
	 */
	@Override
	public void pushLogIntoLoggingservice()
	{
		logger.info("Enter pushLogIntoLoginservice:");
		String response = " ";
		JSONObject logJsonObject = new JSONObject();
		try
		{
			logJsonObject.put("Logs", logArray);
			RestTemplate restTemplate = new RestTemplate();
			HttpHeaders headers = new HttpHeaders();
			headers.setContentType(MediaType.APPLICATION_JSON);
			HttpEntity<String> entity = new HttpEntity<String>(logJsonObject.toString(), headers);
			//TODO: logging response
			response = restTemplate.postForObject(LOGGING_URL, entity, String.class);
			logArray = new JSONArray();
		}
		catch(Exception e)
		{
			// TODO:
			logger.info("Exception in pushLogIntoLoggingservice(): "+ e.getMessage());
		}
		logger.info("Exit pushLogIntoLoginservice:");
	}

	/* 
	 * Method to get batch time in given time zone format
	 */
	@Override
	public String getBatchDateTime()
	{
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ssZZZZ");
		Date datetime = new Date();
		simpleDateFormat.setTimeZone(TimeZone.getTimeZone(TIMEZONE));
		String batchDateTime = simpleDateFormat.format(datetime);
		return batchDateTime;
	}

	/* 
	 * Method to invokePersistentStore
	 */
	public void invokePersistentStore()
	{

	}

}
