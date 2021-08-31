package com.saviynt.ssm.connectorms.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.saviynt.ssm.abstractConnector.BaseConnectorSpecification;
import com.saviynt.ssm.abstractConnector.ConfigDataVo;
import com.saviynt.ssm.abstractConnector.ExposedObject;
import com.saviynt.ssm.abstractConnector.RepositoryReconService;
import com.saviynt.ssm.abstractConnector.SaviyntReadOnlyObject;
import com.saviynt.ssm.abstractConnector.SearchableObject;
import com.saviynt.ssm.abstractConnector.exceptions.ConnectorException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidAttributeValueException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidCredentialException;
import com.saviynt.ssm.abstractConnector.exceptions.MissingKeyException;
import com.saviynt.ssm.abstractConnector.exceptions.OperationTimeoutException;
import com.saviynt.ssm.abstractConnector.utility.GroovyService;
/**
* MySQLDBExampleConnector is an Example custom connector provided to explain how one can build
* their own connector to manage accounts, entitlements etc in any target application via SSM. You need to
* implement various identity lifecycle methods like createAccount(),reconcile() etc from  BaseConnectorSpecification class. 
* These methods will be invoked by SSM while provisioning, deprovisioning, reconciling user, account and access
* to/from the target system.
* Use case - 
* This is a basic custom connector to show case following scenarios :
* 1. This connector connects to MySQL DB ( Connection parameters assigned via setConfig())
* 2. SSM provision the accounts to MySQL DB via createAccount()
* 3. SSM reconcile the accounts back into SSM via reconcile ()
**/
public class MySQLDBExampleConnector extends BaseConnectorSpecification {
	private static final long serialVersionUID = 1L;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	/**
	 * to retrieve connector display name.
	 *
	 * @return the string
	 */
	@Override
	public String displayName() {

		return "MySQLDBExampleConnector";
	}
	/**
	 *  to retrieve connector version.
	 *
	 * @return the string
	 */
	@Override
	public String version() {
		return "1.0";
	}
	/**
	 * to test the connection
	 * Example : To test the connection , refer to the below steps
	 * step 1  : retrieve connection attributes from configData/Data
	 * step 2  : connect to target system using JDBC connection
	 * step 3  : return true if connection is successful
	 *
     * @param configData the configData This is a metadata that contains the details of the information required 
	          and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime.
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 * @throws InvalidCredentialException the invalid credential exception
	 * @throws InvalidAttributeValueException the invalid attribute value exception
	 * @throws OperationTimeoutException the operation timeout exception
	 * @throws MissingKeyException the missing key exception
	 */
	@Override
	public Boolean test(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException,
			InvalidCredentialException, InvalidAttributeValueException, OperationTimeoutException, MissingKeyException {
		return true;

	}
	/**
	 * to set the config with attributes needed for creating a connection to the target system from SSM.
	 * The attributes defined in configData in setConfig are the attributes that would dynamically
	 * populate on the connection creation UI under SSM to be inputed.
	 * Connection attributes can be added in following way:
	 * Example: List<String> connectionAttributes = configData.getConnectionAttributes();
	 * connectionAttributes.add("drivername");
	 * 
	 * Connection attributes that need to be encrypted can be added to configData as below:
	 * Example : List<String> encryptedConnectionAttributes = configData.getEncryptedConnectionAttributes();
	 *			 encryptedConnectionAttributes.add("Password");
	 *
	 * Description or details of the format in which the config attributes are supposed to be inputed from
	 * the UI can be added in configData as below:
	 * JSONObject jsonObject = new JSONObject(connectionAttributesDescription);
	 * jsonObject.put("Password", "Provide password to connect with application");
	 * jsonObject.put("CreateUserJSON", "SAMPLE JSON {}");
	 * configData.getConnectionAttributesDescription().setConnectionAttributesDescription(jsonObject.toString()); 
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
	 *		  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	 *        This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 */
	@Override
	public void setConfig(ConfigDataVo configData) {
		List<String> connectionAttributes = configData.getConnectionAttributes();
		connectionAttributes.add("drivername");
		connectionAttributes.add("username");

		connectionAttributes.add("password");
		connectionAttributes.add("url");
		connectionAttributes.add("ReconcileJSON");
		connectionAttributes.add("RemoveAccountJSON");
		connectionAttributes.add("RemoveAccessToAccountJSON");
		connectionAttributes.add("AddAccessToAccountJSON");
		connectionAttributes.add("UpdateAccountJSON");
		connectionAttributes.add("CreateAccountJSON");
		connectionAttributes.add("LockAccountJSON");
		connectionAttributes.add("DisableAccountJSON");
		connectionAttributes.add("EnableAccountJSON");
		connectionAttributes.add("TerminateAccountJSON");
		connectionAttributes.add("UnlockAccountJSON");
		connectionAttributes.add("CreateUserJSON");
		connectionAttributes.add("UpdateUserJSON");
		connectionAttributes.add("UpdateEntitlementJSON");
		connectionAttributes.add("CreateEntitlementJSON");
		connectionAttributes.add("ChangePasswordJSON");
 
		List<String> requiredConnectionAttributes = configData.getRequiredConnectionAttributes();
		requiredConnectionAttributes.add("drivername");
		requiredConnectionAttributes.add("username");
		requiredConnectionAttributes.add("password");
		requiredConnectionAttributes.add("url");

		List<String> encryptedConnectionAttributes = configData.getEncryptedConnectionAttributes();
		encryptedConnectionAttributes.add("password");

		String ConnectionAttributesDescription = configData.getConnectionAttributesDescription();

		JSONObject jsonObject = new JSONObject(ConnectionAttributesDescription);

		for (String k : connectionAttributes) {
			if (k.endsWith("JSON")) {

			} else {
				jsonObject.put(k, "Value of  " + k);
			}
		}
		jsonObject.put("ReconcileJSON",
				"SAMPLE JSON For    {'updatedUser':'SaviyntAdmin','reconType':'fullrecon','query':'  select * from accountrecon ','endpointId':61,'formatterClass':'com.saviynt.ssm.abstractConnector.AbstractFormatter','mapper':{'accounts':[{'saviyntproperty':'name','sourceproperty':'${accountName}'}],'account_attributes':[{'saviyntproperty':'attribute_name','sourceproperty':'${Emailaddress}'}],'account_entitlements':[{'saviyntproperty':'entitlementtype','sourceproperty':'${group}'},{'saviyntproperty':'entitlement_value','sourceproperty':'${entitlementVal}'},{'saviyntproperty':'name','sourceproperty':'${accountName}'}]}}");
		jsonObject.put("RemoveAccessToAccountJSON", "SAMPLE JSON For    {'query':['Valid Sql Query']} ");
		jsonObject.put("RemoveAccountJSON", "SAMPLE JSON For    {'query':['Valid Sql Query']}");
		jsonObject.put("AddAccessToAccountJSON", "SAMPLE JSON  {'query':['Valid Sql Query']}");
		jsonObject.put("CreateAccountJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("CreateUserJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("UpdateUserJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("UpdateEntitlementJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("CreateEntitlementJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");
		jsonObject.put("ChangePasswordJSON", "SAMPLE JSON   {'query':['Valid Sql Query']}");

			configData.setConnectionAttributesDescription(jsonObject.toString());		
	}
	/**
	 * to process reconcile for users and accounts
	 * Example : to process reconcile for users and accounts , refer to the below steps
	 * step 1  : retrieve connection attributes from configData/Data
	 * step 2  : collect the data(Account,Users,Entitlements) from target system
	 * step 3  : set the data into the format accepted by connector framework's RepositoryReconService.notify()
	             sample format: finalData=[[{ACCOUNT.CUSTOMPROPERTY2=XXXX, ACCOUNT.CUSTOMPROPERTY1=XXXX, ACCOUNT.NAME=XXXX},
		                                  {ENTITLEMENT.NAME=XXXX, ENTITLEMENT.ENTITLEMENTTYPE=XXXX, ENTITLEMENT.ENTITLEMENT_VALUE=XXXX},
		 	 							  {ACCOUNT_ATTRIBUTES.ATTRIBUTE_VALUE=XXXX, ACCOUNT_ATTRIBUTES.NAME=XXXX, ACCOUNT_ATTRIBUTES.ATTRIBUTE_NAME=XXXX},
			 							  {USERS.USERNAME=XXXX}]]
	 * step 4 : call connector framework's RepositoryReconService.notify() with finalData as input param from step 3 for SSM to process recon
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
	          and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
     * @param  formatterClass the formatter class - This is for future implementation hence ignore it for now 
	 * @throws ConnectorException the connector exception
	 */
 	@Override
	public void reconcile(Map<String, Object> configData, Map<String, Object> data, String formatterClass)
			throws ConnectorException {
		
 		logger.debug("Enter DatabaseConnectorService reconcile");
		
		try {
			//process account reconciliation. IMPORTABLE_OBJECT is obtained from SSM and it suggest whether
			//user or account  recon is happening. Values avaialble - USER, ACCOUNT.
		if (data.get("IMPORTABLE_OBJECT") != null && data.get("IMPORTABLE_OBJECT").equals("ACCOUNT")) {
			accountReconcile(configData, data);
			logger.debug("End DatabaseConnectorService reconcile");
			return;
		}
		
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);
		} 

	}
	/**
	 * to establish connection to the target system
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
	          and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @return
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	private static Connection getConnection(Map<String, Object> configData)
			throws ClassNotFoundException, SQLException {

		Class.forName(configData.get("drivername").toString());
		return DriverManager.getConnection(configData.get("url").toString(), configData.get("username").toString(),
				configData.get("password").toString());

	}
	/**
	 * to check existing record for the input object USER, ACCOUNT, ENTITLEMENT, ACCOUNT_ENTITLEMENT in SSM
	 * Example : to check existing record for the input object(for account) , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : set the data with filters if any
	 * step 3 : call getObjectList
	 * step 4 : return true if object exists
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
	          and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
     * @param serachableObject for serachableObject enabled for the entities USER, ACCOUNT, ENTITLEMENT, ACCOUNT_ENTITLEMENT
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Boolean checkExisting(Map<String, Object> configData, Map<String, Object> data,
			SearchableObject serachableObject) throws ConnectorException {
		Boolean recordFound=false;
		//Connect to target System With Config data 
		//Check in SSM to find user with firstname
		Map<String,Object> userMap = new HashMap<String,Object>();
		userMap.put("firstname", "firstnameissaviynt");
		//call SaviyntReadOnlyObject.getObjectList(ExposedObject sObject,Map<String, Object> filter,Integer firstResult,Integer maxResult) with below arguments in the below order
		     // ExposedObject sObject  - USERS  in this use case
		     // filter is the criteria to retrieve user object which is userMap in this use case
		     // Integer firstResult - row number  which is 1 in this use case
		     // Integer maxResult - maximum results 
        //List resultList = SaviyntReadOnlyObject.getObjectList(ExposedObject.USERS, userMap,1,2);
        //Return true if resultList >0
		return recordFound;
	}
	/**
	 * to create account in the target system 
	 * Example : to create account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to create account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties from the target system to be updated in SSM for that account.
	           Example : Match the corresponding column of SSM with target system data and build sampleMetadata.
	           map sampleMetadata to provisioningData key in the result map as below and return
	           map.put("provisioningData",sampleMetadata)
	           metadata sample format : {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	           This is optional filed. If no metadata needs to updated in SSM, it can be set to null
	           Example : map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Map createAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService createAccount");
 		Map map = new HashMap();
 		Connection con = null;
 
		try {
			/*
			 * Use case : 
			 * Create Account in MySQL DB using the query specified by the user in 
			 * the CreateAccountJSON on this connector's connection UI in SSM. 
			 * When the account is created in DB, "accountidentifier" column's value for this account is generated autommatically
			 * by the MySQL DB in this example.Since, this column's value is not present in SSM for this account, it is sent 
			 * back to SSM by mapping it to "customproperty1" column of ACCOUNT Table. Please note that returning a map is optional.
			 * It has be set to null if no data has to be returned to SSM
			 * Please note that we are sending back the entire account data for the current account to SSM
			 */

			//persist account data into target system 
			executeInputQuery(configData, data, "CreateAccountJSON");
						
			//retrieve data from target to send back to SSM. 
			
			//Query to get the account record with name  equals to current ACCOUNTNAME retrieved from SSM
			String query = "Select * from accounts where name = ? ";
			con = getConnection(data);
     		JSONObject jsonObject = new JSONObject(data.get("ReconcileJSON").toString());
    		Map<String, Object> dataForMap = jsonObject.toMap();
			Map<String, Object> mapperMap = (Map<String, Object>) dataForMap.get("mapper");
			
			//The is the account name of the account being processed by SSM. This is set by SSM during provisioning job invocation
			String accountName = data.get("ACCOUNTNAME").toString();
			
			//call getMetaDataFromTableAndMatchProperties() to retrieve the value of accountIdentifier column 
			//of this account and map it to customproperty1
			//Note : We are sending back the entire account data of the current account to SSM which in turn will have 
			//the mapping of the new accountIdentifier column as well
			map.put("provisioningData", getMetaDataFromTableAndMatchProperties(con, mapperMap, query, accountName));
			
			logger.debug("Exit DatabaseConnectorService createAccount");
		} catch (Exception ex) {
			logger.error(ex.getMessage(), ex);
			throw new ConnectorException(ex.getMessage());
		}
		finally {
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
				throw new ConnectorException(e);
			}
		}

	}
		return map;
	}
	/**
	 * to update account in the target system
	 * Example : to update account in the target system , refer to the below steps
	 * step 1  : retrieve connection attributes from configData/Data
	 * step 2  : connect to the target system
	 * step 3  : execute the query/process with the required input to update account in the target system
	 * step 4  : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties from the target system to be updated in SSM for that account.
	           Example:Match the corresponding column of SSM with target system data and build sampleMetadata.
	           map sampleMetadata to provisioningData key in the result map as below and return
	           map.put("provisioningData",sampleMetadata)
	           metadata sample format : {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	           This is optional filed. If no metadata needs to updated in SSM, it can be set to null
	           Example : map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map updateAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService updateAccount");
		Map map = new HashMap();
		try {
			//persist data into target system
			executeInputQuery(configData, data, "UpdateAccountJSON");
			//retrieve data from target system that needs to be send back to SSM 
		 	//set metadata in a map like : {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService updateAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to lock the account in target system
	 * Example : to lock account in the target system , refer to the below steps
	 * step 1  : retrieve connection attributes from configData/Data
	 * step 2  : connect to the target system
	 * step 3  : execute the query/process with the required input to lock account in the target system
	 * step 4  : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map lockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService lockAccount");
		Map map = new HashMap();
		try {
		    executeInputQuery(configData, data, "LockAccountJSON");
		  //return null in below Map   
			map.put("provisioningData", null);
		    logger.debug("End DatabaseConnectorService lockAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to disable account in the target system
	 * Example : to disable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to disable account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
     * @throws ConnectorException the connector exception
	 */
	@Override
	public Map disableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		logger.debug("Enter DatabaseConnectorService disableAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "DisableAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.debug("End DatabaseConnectorService disableAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
		}
	/**
	 * to unlock account in the target system
	 * Example : to disable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to disable account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
     * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map unLockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService unLockAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "unLockAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.info("End DatabaseConnectorService unLockAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	 /**
     * to enable account in the target system
	 * Example : to enable account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to enable account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map enableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService enableAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "EnableAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.debug("End DatabaseConnectorService enableAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to remove account in the target system
	 * Example : to remove account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to remove account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)}

	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map removeAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		logger.debug("Enter DatabaseConnectorService removeAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "RemoveAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.debug("End DatabaseConnectorService removeAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;

	}
	/**
	 * to terminate account in the target system
	 * Example : to terminate account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to terminate account in the target system
	 * step 4 : return the number of records updated
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the integer number of accounts terminated
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Integer terminateAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.info("Enter DatabaseConnectorService terminateAccount");
		Integer resultCount = 0;
		try {
			resultCount = executeInputQuery(configData, data, "TerminateAccountJSON");
			logger.info("End DatabaseConnectorService terminateAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount ;
	}
	/**
	 * to add access to account in the target system
	 * Example : to add access to account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to add access to account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map addAccessToAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService AddAccessToAccountJSON");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "AddAccessToAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.debug("End DatabaseConnectorService AddAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to remove access to account in the target system
	 * Example : to remove access to account in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to remove access to account in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	                Example: map.put("provisioningData",null)
     * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map removeAccessToAccount(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService RemoveAccessToAccountJSON");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "RemoveAccessToAccountJSON");
			//return null in below Map   
			map.put("provisioningData", null);
			logger.debug("End DatabaseConnectorService RemoveAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to create the entitlement in target system
	 * Example : to create entitlement in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to create entitlement in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties from the target system to be updated in SSM for that account.
	           Example:Match the corresponding column of SSM with target system data and build sampleMetadata.
	           map sampleMetadata to provisioningData key in the result map as below and return
	           map.put("provisioningData",sampleMetadata)
	           metadata sample format : {ENTITLEMENT.COLUMNNAME1=XXXX, ENTITLEMENT.COLUMNNAME2=XXXX, ENTITLEMENT.COLUMNNAME3=XXXX}
     * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings("rawtypes")
	@Override
	public Map createEntitlement(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService createEntitlement");

		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "CreateEntitlementJSON");
		    // retrieve data from target system that needs to be sent back to SSM 
		 	// set metadata in a map like : {ENTITLEMENT.COLUMNNAME1=XXXX, ENTITLEMENT.COLUMNNAME2=XXXX, ENTITLEMENT.COLUMNNAME3=XXXX}
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService createEntitlement");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to update the entitlement in target system
	 * Example : to update entitlement in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to update entitlement in the target system
	 * step 4 : return the Map with metadata as explained in parameter description
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties from the target system to be updated in SSM for that account.
	           Example:Match the corresponding column of SSM with target system data and build sampleMetadata.
	           map sampleMetadata to provisioningData key in the result map as below and return
	           map.put("provisioningData",sampleMetadata)
	           metadata sample format : {ENTITLEMENT.COLUMNNAME1=XXXX, ENTITLEMENT.COLUMNNAME2=XXXX, ENTITLEMENT.COLUMNNAME3=XXXX}
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Map updateEntitlement(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService updateEntitlement");
		Map map = new HashMap();
		try {
			 executeInputQuery(configData, data, "UpdateEntitlementJSON");
			 // retrieve data from target and send it back to SSM 
			 /** set metadata **/
			 //map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService updateEntitlement");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to change password in the target system
	 * Example : to change password in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to change password in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean changePassword(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService changePassword");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "ChangePasswordJSON");
			logger.info("End DatabaseConnectorService changePassword");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	/**
     * to create user in the target system
	 * Example : to create user in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to create user in the target system
	 * step 4 : return true if successful
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
     */
	@Override
	public Boolean createUser(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService createUser");
		Integer resultCount = 0;

		try {
			resultCount = executeInputQuery(configData, data, "CreateUserJSON");
			logger.info("End DatabaseConnectorService createUser");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount > 0 ? true : false;
	}
	 /**
   	 * to update user in the target system
   	 * Example : to update user in the target system , refer to the below steps
   	 * step 1 : retrieve connection attributes from configData/Data
   	 * step 2 : connect to the target system
   	 * step 3 : execute the query/process with the required input to update user in the target system
   	 * step 4 : return the number of records updated
   	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
   	 * @return the integer number of users updated
   	 * @throws ConnectorException the connector exception
   	 */
	@Override
	public Integer updateUser(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService updateUser");
		Integer resultCount = 0;
		try {
			resultCount = executeInputQuery(configData, data, "UpdateUserJSON");
			logger.info("End DatabaseConnectorService updateUser");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return resultCount;
	}
	/**
	 * to validate credentials of the given input from connection
	 * Example : to validate credentials in the target system , refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the query/process with the required input to validate credentials in the target system
	 * step 4 : return true if successful, false if failure
	 * 
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean validateCredentials(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// write your own logic to validate credentials set in configData with the target system
		return null;
	}
	/**
	 * This is for FUTURE implementation hence ignore for now
	 * to get the summary of number of records for the given input object such as accounts.It provides number of accounts,users etc
	 *
     * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return the summary map with object and count as key ,value. This is for future implementation hence ignore for now
	 */
	@Override
	public Map<String, Object> getSummary(Map<String, Object> configData, Map<String, Object> data) {
		Map<String, Object> map = new HashMap<String, Object>();
		// future implementation
		return map;
	}
	/**
	 * Read database properties for batch size
	 * 
	 * @return integer holds the number of batches
	 */
	private Integer batchSize() {

		int batchSize = 0;
		try (InputStream input = getClass().getClassLoader().getResourceAsStream("databaseconfig.properties")) {

			Properties prop = new Properties();
			if (input == null) {
				System.out.println("unable to find " + input);
			}
			prop.load(input);
			batchSize = Integer.valueOf(prop.getProperty("jdbc.batch.size"));
			if (batchSize <= 0) {

				batchSize = Integer.MIN_VALUE;
			}

		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return batchSize;
	}
	/**
	 * executeInputQuery is the query to be executed in target system
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @param key
	 * @return Integer is the number of queries executed
	 */
	private static Integer executeInputQuery(Map<String, Object> configData, Map<String, Object> data, String key) {

		Connection con = null;
		Integer resultCount = 0;

		try {
			con = getConnection(data);
			if (data.containsKey(key)) {
				JSONObject jsonObject = new JSONObject(data.get(key).toString());
				JSONArray query = jsonObject.getJSONArray("query");
				Statement stmt = con.createStatement();
				for (int i = 0; i < query.length(); i++) {
					String queryStr = GroovyService.convertTemplateToString(query.getString(i), data);
					stmt.addBatch(queryStr);
				}
				resultCount = stmt.executeBatch().length;

			}

			return resultCount;
		} catch (Exception e) {
			throw new ConnectorException(e);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					throw new ConnectorException(e);
				}
			}

		}
	}
	/**
	 * set properties from target system to saviynt properties from input JSON
  	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @param con connection details
	 * @param lastRunDate
	 * @return
	 */
	@SuppressWarnings("unchecked")
	private List<List<Map<String, Object>>> mapPropertiesToSaviynt(Map<String, Object> data, Connection con,
			Date lastRunDate) {

		String queryInput = data.get("query").toString();
		String query = "";
		if (lastRunDate != null) {
			query = queryInput.concat(" where updatedate >= " + lastRunDate);
		} else {
			query = queryInput;
		}
		List<List<Map<String, Object>>> accountsAllDataMAp = new ArrayList<List<Map<String, Object>>>();
		try {

			Map<String, Object> mapperMap = (Map<String, Object>) data.get("mapper");
			accountsAllDataMAp = getDataFromTableAndMatchProperties(con, mapperMap, query);

		} catch (Exception e) {

			logger.error("Error" + e.getMessage());
		}
		return accountsAllDataMAp;

	}
	/**
	 * getDataFromTableAndMatchProperties to match data fetched from target system to saviynt
	 * @param con connection details
	 * @param mapperMap contains system match properties from saviynt to target 
	 * @param query to be executed in target system
	 * @return List
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private List<List<Map<String, Object>>> getDataFromTableAndMatchProperties(Connection con,
			Map<String, Object> mapperMap, String query) throws Exception {

		Statement stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		stmt.setFetchSize(batchSize());
		ResultSet rs = stmt.executeQuery(query);
		ResultSetMetaData rsmd = rs.getMetaData();

		List<List<Map<String, Object>>> allAccountsFinalList = new ArrayList<List<Map<String, Object>>>();

		while (rs.next()) {

			Map<String, Object> resultsetMap = new HashMap<String, Object>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				String column = rsmd.getColumnLabel(i);
				resultsetMap.putIfAbsent(column, rs.getString(i));
			}
			List<Map<String, Object>> tempList = new ArrayList<Map<String, Object>>();

			for (String maperKey : mapperMap.keySet()) {

				Map<String, Object> oneRow = new HashMap<String, Object>();

				List<Map<String, String>> valMap = (List<Map<String, String>>) mapperMap.get(maperKey);
				for (Map<String, String> columnPropertyMap : valMap) {
					oneRow.put(maperKey.toUpperCase() + "." + columnPropertyMap.get("saviyntproperty").toUpperCase(),
							GroovyService.convertTemplateToString(columnPropertyMap.get("sourceproperty"),
									resultsetMap));

				}
				
				tempList.add(oneRow);

			}
			allAccountsFinalList.add(tempList);
		}

		return allAccountsFinalList;
	}
	/**
	 * to set data to the map from the target system by matching saviynt and target system properties 
	 * @param con contains connection details for the target system
	 * @param mapperMap contains system match properties from saviynt to target 
	 * @param query to be executed to fetch data from target system
	 * @param name input param which is account name
	 * @return Map
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	private Map<String, Object> getMetaDataFromTableAndMatchProperties(Connection con,
			Map<String, Object> mapperMap, String query,String name) throws Exception {

		PreparedStatement stmt = con.prepareStatement(query);
 		stmt.setString(1, name);
		ResultSet rs = stmt.executeQuery();
		ResultSetMetaData rsmd = rs.getMetaData();

		Map<String, Object> oneRowResultMap = new HashMap<String, Object>();

		while (rs.next()) {

			Map<String, Object> resultsetMap = new HashMap<String, Object>();
			for (int i = 1; i <= rsmd.getColumnCount(); i++) {

				String column = rsmd.getColumnLabel(i);
				resultsetMap.putIfAbsent(column, rs.getString(i));
			}
			for (String mapperKey : mapperMap.keySet()) {

 				if(mapperKey.equalsIgnoreCase("ACCOUNT")) {
 					
 					List<Map<String, String>> valMap = (List<Map<String, String>>) mapperMap.get(mapperKey);
 					for (Map<String, String> columnPropertyMap : valMap) {
 						oneRowResultMap.put(mapperKey.toUpperCase() + "." + columnPropertyMap.get("saviyntproperty").toUpperCase(),
 								GroovyService.convertTemplateToString(columnPropertyMap.get("sourceproperty"),
 										resultsetMap));
 					}
 					break;
 				}
			}
  		}

		return oneRowResultMap;
	}
	/**
	 * accountReconcile to process account reconcile
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map
	 * @throws ConnectorException
	 */
	private void accountReconcile(Map<String, Object> configData,
			Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService accountReconcile" + data);

		try {
			logger.debug("dataFromEcm" + data);
			 processAccountReconcile(configData, data,
					Long.valueOf(data.get("endpointId").toString()));

			logger.info("End DatabaseConnectorService accountReconcile");

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);
		}
 	}
	/**
	 * processAccountReconcile to process account reconcile
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @param endPointId
	 * @return
	 */
	private void processAccountReconcile(Map<String, Object> configData,
			Map<String, Object> data, Long endPointId) {
	 
		JSONObject jsonObject = new JSONObject(data.get("ReconcileJSON").toString());
		Map<String, Object> tempdata = jsonObject.toMap();
		Connection con = null;
		Date lastRunDate = null;

		try {
			con = getConnection(data);
			 
			List<List<Map<String, Object>>> temp = mapPropertiesToSaviynt(tempdata, con, lastRunDate);
			
			// notify of connector MS process the reconcile data
			/*
			 * temp sample format: [[{ACCOUNT.CUSTOMPROPERTY2=XXXX, ACCOUNT.CUSTOMPROPERTY1=XXXX, ACCOUNT.NAME=XXXX},
			 *                          {ENTITLEMENT.NAME=XXXX, ENTITLEMENT.ENTITLEMENTTYPE=XXXX, ENTITLEMENT.ENTITLEMENT_VALUE=XXXX},
			 *                           {ACCOUNT_ATTRIBUTES.ATTRIBUTE_VALUE=XXXX, ACCOUNT_ATTRIBUTES.NAME=XXXX, ACCOUNT_ATTRIBUTES.ATTRIBUTE_NAME=XXXX},
			 *                            {USERS.USERNAME=XXXX}]]
			 * endPointId : Retrieved from data object                           
			 */
			RepositoryReconService.notify(temp, endPointId, null, data);

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);

		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					throw new ConnectorException(e);
				}
			}

		}
	}
	/**
	 * to provide the firefighterId access to a system/application in target system for the inputed create account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {null}
	 * Example : to add firefighterIdGrantAccess(firefighterIdGrantAccess is invoked when provisioning job is triggered in SSM) to account in the target system ,
	 			 refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the process with the required input to add access in the target system
	 * step 4 : return the map with metadata
	 * @param configData the configData This is a metadata that contains the details of the information required
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations.
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime.
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER")
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 	       Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Map firefighterIdGrantAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		//write your own logic to grant firefighterId Access in target System 
		Map map = new HashMap();
		//return null in below Map   
		map.put("provisioningData", null);
		return map;
	}
	/**
	 * to provide the firefighterId instance access to a system/application in target system for the inputed create account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {null}
	 * Example : to provide firefighterIdInstanceGrantAccess(firefighterIdInstanceGrantAccess is invoked immediately upon the task creation in SSM) to account in the target system ,
	 			 refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the process with the required input to add access in the target system
	 * step 4 : return the map with metadata
	 *
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	       Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Map firefighterIdInstanceGrantAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		//write your own logic to grant firefighterIdInstance Access in target System 
		Map map = new HashMap();
		//return null in below Map   
		map.put("provisioningData", null);
		return map;
	}
	/**
	 * to remove the firefighterId instance access to a system/application in target system for the inputed create account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {null}
	 * Example : to revoke firefighteridInstanceaccess(firefighterIdInstanceRevokeAccess is invoked immediately upon the task creation in SSM) to account in the target system ,
	 			 refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the process with the required input to revoke access in the target system
	 * step 4 : return the map with metadata
	 * @param configData the configData This is a metadata that contains the details of the information required 
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations. 
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime. 
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER") 
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 *	       Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Map firefighterIdInstanceRevokeAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		//write your own logic to revoke access to firefighterId Instance  in target System 
		Map map = new HashMap();
		//return null in below Map   
		map.put("provisioningData", null);
		
		return map;
	}
	/**
	 * to remove the firefighterId access to a system/application in target system for the inputed create account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {null}
	 * Example : to revoke firefighteridaccess(firefighterIdRevokeAccess is invoked when provisioning job is triggered in SSM) to account in the target system ,
	 			 refer to the below steps
	 * step 1 : retrieve connection attributes from configData/Data
	 * step 2 : connect to the target system
	 * step 3 : execute the process with the required input to revoke access in the target system
	 * step 4 : return the map with metadata
	 * @param configData the configData This is a metadata that contains the details of the information required
			  and configurations needed for establishing the connectivity to the target system and for doing provisioning and reconciliation operations.
	          This is defined in setConfig().These appear as JSON or fields on the UI that have to be inputed at the time of creating the connection for this connector in SSM
	 * @param data contains the values (input details) of the JSON attributes/fields specified at the time of creating the connection for this connector in SSM UI.
              current user/task/entitlement/account data referred in inputed JSON are dynamically populated at the runtime.
			  Along with connection attributes, this parameter also contains some additional information (key value pairs) that can be used during
              provisioning,reconciliation etc. e.g IMPORTABLE_OBJECT - This signifies whether account recon or user recon is happening. Valid values ("ACCOUNT","USER")
              endpointId -  contains endpoint Id for the endpoint corresponding to this connector
	 * @return Map which consists of metadata to be updated in SSM.This is for future implementation hence set it to null for now
	 	       Example: map.put("provisioningData",null)
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Map firefighterIdRevokeAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {

		//write your own logic to grant revoke access to firefighterId in target System 
		Map map = new HashMap();
		//return null in below Map   
		map.put("provisioningData", null);
		
		return map;
	}
}
