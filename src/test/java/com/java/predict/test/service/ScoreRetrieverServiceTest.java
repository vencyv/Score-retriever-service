package com.java.predict.test.service;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;

import org.json.JSONArray;
import org.junit.Assert;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.java.predict.service.ScoreRetrieverServiceImpl;
import com.java.predict.utility.ErrorUtil;

/*@ContextConfiguration(classes = {ScoreRetrieverServiceImpl.class,ErrorUtil.class,RetryTemplate.class})
@WebAppConfiguration*/
@SpringBootTest(classes = {ScoreRetrieverServiceImpl.class,ErrorUtil.class,RetryTemplate.class})
public class ScoreRetrieverServiceTest extends AbstractTestNGSpringContextTests 
{
	
	@SpyBean
	@Autowired
	private ScoreRetrieverServiceImpl scoreRetrieverServiceImpl;
	
	@DataProvider(name = "fetchDataFromStorageBlobDataProvider")
	public static Object[][] fetchDataFromStorageBlobDataProvider()
	{
		return new Object[][]{
			{"{\"ModelName\": \"Trauma\"\"ModelVersion\": \"v1\"}","Exception"},
			{"{}","Exception"},
			{"{\"ModelName\": \"Trauma\",\"ModelVersion\": \"v1\",\"PatientID\": \"E2731\",\"PatientAge\": 67,\"BatchDateTime: \"2018-06-06T12:00:00-0500\",\"TransformedReference\": \"Patients/E3278/2018-06-12T04:03:26-0500/Patient-Records/Transformed/trauma.json\",\"ScoreReference\": \"Patients/E3278/2018-06-12T04:34:26-0500/Score/trauma.json\",\"API\": [{\"Score-retriever-stage\": [{\"Name\": \"AddFlowsheetValue\",\"URL\": \"https://apporchard.epic.com/interconnect-ao85prd-username/api/epic/2011/Clinical/Patient/ADDFLOWSHEETVALUE/FlowsheetValue\",\"Type\": \"RESTful\",\"MethodType\": \"POST\",\"Parameters\": [\"PatientID\", \"PatientIDType\", \"ContactID\", \"ContactIDType\", \"UserID\", \"UserIDType\", \"FlowsheetID\", \"FlowsheetIDType\", \"Value\", \"InstantValueTaken\", \"FlowsheetTemplateID\", \"FlowsheetTemplateIDType\"],\"ParametersValue\": [\"E2731\", \"INTERNAL\", \"58706\", \"{CONTACTIDTYPE}\", \"1\", \"EXTERNAL\", \"1\", \"INTERNAL\", \"{SCORE}\", \"2018-06-12T23:12:00Z\", \"{FLOWSHEETTEMPLATEID}\", \"{FLOWSHEETTEMPLATEIDTYPE}\"]}]}]}","Exception"},
		};
	}
	
	@Test(dataProvider="fetchDataFromStorageBlobDataProvider")
	public void getPredictScoreTest(String queueMsg, String expectedResult)
	{
		String result = "";
		String transformedReference = "Patients/E3278/2018-06-12T04:03:26-0500/Patient-Records/Transformed/traumaTransformed.json";
		String transformedJson = "[{\"PatientID\":12345,\"Patient.Age\":24,\"ProblemList\":[{\"Problem.DiagnosisIDs\":{\"Problem.DiagnosisIDs.ID\":1,\"Problem.DiagnosisIDs.IDType\":\"Internal\"},\"Problem.ProblemName\":\"ABRASION\",\"Problem.DateNoted\":\"2009-01-01T01:00:00\",\"Problem.DateEntered\":\"2009-01-01T01:00:00\",\"Problem.Class\":\"Acute\"}],\"ResultComponents\":[{\"Component.ComponentID\":1,\"Component.Abnormality.Title\":\"Abnormal\",\"Component.PrioritizedDate\":\"3/23/2015\",\"Component.PrioritizedTime\":\"9:00 AM\",\"Component.ReferenceRange\":\"4.7-6.1\",\"Component.Status.Title\":\"Final\",\"Component.Units\":\"cells/mcL\",\"Component.Value\":\"4.01\"}],\"MedicationOrders\":[{\"MedicationOrder.DispenseQuantity\":5,\"MedicationOrder.DispenseQuantityUnit\":\"tablets\",\"MedicationOrder.Dose\":100,\"MedicationOrder.DoseUnit.Number\":1,\"MedicationOrder.DoseUnit.Title\":\"mL\",\"MedicationOrder.DoseAdminDuration\":1,\"MedicationOrder.DoseAdminDurationUnit.Number\":1,\"MedicationOrder.DoseAdminDurationUnit.Title\":\"Hours\",\"MedicationOrder.EndDateTime\":\"2009-01-01T00:00:00\",\"MedicationOrder.Frequency.DisplayName\":\"Continuous\",\"MedicationOrder.Name\":\"Dextrose 5 % solution 100 mL\",\"MedicationOrder.Route.Name\":\"Intravenous\",\"MedicationOrder.StartDateTime\":\"2009-01-01T01:00:00\"}],\"Contacts\":[{\"Contact.ID\":54321,\"Contact.DateTime\":\"2009-01-01T00:00:00\",\"Contact.DepartmentID\":1,\"Contact.DepartmentName\":\"ED\",\"FlowsheetRows\":[{\"FlowsheetRow.Name\":\"Glasgow Coma Scale Score\",\"FlowsheetRow.FlowsheetRowID\":1,\"FlowsheetRow.RawValue\":3,\"FlowsheetRow.Instant\":\"2009-01-01T01:00:00\"}]}]}]";
		String scoreReference = "Patients/E3278/2018-06-12T04:03:26-0500/Score/trauma.json";
		String score = "12.5";
		String api = "[{\"Score-retriever-stage\":[{\"MethodType\":\"POST\",\"Type\":\"RESTful\",\"Parameters\":[\"PatientID\",\"PatientIDType\",\"ContactID\",\"ContactIDType\",\"UserID\",\"UserIDType\",\"FlowsheetID\",\"FlowsheetIDType\",\"Value\",\"InstantValueTaken\",\"FlowsheetTemplateID\",\"FlowsheetTemplateIDType\"],\"ParametersValue\":[\"E2731\",\"INTERNAL\",\"58706\",\"{CONTACTIDTYPE}\",\"1\",\"EXTERNAL\",\"1\",\"INTERNAL\",\"{SCORE}\",\"2018-06-12T23:12:00Z\",\"{FLOWSHEETTEMPLATEID}\",\"{FLOWSHEETTEMPLATEIDTYPE}\"],\"URL\":\"https:\\apporchard.epic.com\\interconnect-ao85prd-username\\api\\epic\\2011\\Clinical\\Patient\\ADDFLOWSHEETVALUE\\FlowsheetValue\",\"Name\":\"AddFlowsheetValue\"}]}]";
		String messageID = "";
		JSONArray apiJsonArray = new JSONArray();
		apiJsonArray.put(api);
		try
		{
			
			doReturn(transformedJson).when(scoreRetrieverServiceImpl).fetchDataFromStorageBlob(transformedReference);
			doReturn(score).when(scoreRetrieverServiceImpl).callModelService(transformedJson);
			doNothing().when(scoreRetrieverServiceImpl).updateDataIntoStorageBlob(scoreReference, score);
			doNothing().when(scoreRetrieverServiceImpl).updateDataIntoEpic(apiJsonArray, score);	
			
			scoreRetrieverServiceImpl.predictScore(queueMsg, messageID);
			result = "Suuccess";		
		}
		catch (Exception e)
		{
			result = "Exception";
		}
		
		Assert.assertEquals(result, expectedResult);
	}

	
}
