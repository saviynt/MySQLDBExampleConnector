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
import com.saviynt.ssm.abstractConnector.RepositoryReconService;
import com.saviynt.ssm.abstractConnector.SearchableObject;
import com.saviynt.ssm.abstractConnector.exceptions.ConnectorException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidAttributeValueException;
import com.saviynt.ssm.abstractConnector.exceptions.InvalidCredentialException;
import com.saviynt.ssm.abstractConnector.exceptions.MissingKeyException;
import com.saviynt.ssm.abstractConnector.exceptions.OperationTimeoutException;
import com.saviynt.ssm.abstractConnector.utility.GroovyService;
 

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
		return "2.2";
	}
	/**
	 * to test the connection of the input target system configured in connection on SSM
	 * 
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the configured attributes through setConfig from connection 
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception throws connector exception
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
	 * to process reconciliation for users and accounts by extracting the input for Recon from SSM Connection attributes when initiated a Recon job in SSM
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the configured attributes through setConfig from connection 
	 * @param formatterClass the formatter class
	 */
 	@Override
	public void reconcile(Map<String, Object> configData, Map<String, Object> data, String formatterClass)
			throws ConnectorException {
		
 		logger.debug("Enter DatabaseConnectorService reconcile");
		
		try {
			//process account reconciliation. IMPORTABLE_OBJECT contains ACCOUNT or USER from SSM.
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
	 * @param configData
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
	 * to check existing record for the input object such as users inputed from SSM connection attributes.
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @param serachableObject the serachable object to retrieve the inputed entity information from the source system
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean checkExisting(Map<String, Object> configData, Map<String, Object> data,
			SearchableObject serachableObject) throws ConnectorException {
 		//Connect to target System With Config data 
		//Check in target System 
		//Return true or false
		return null;
	}
	/**
	 * to create the account in target system for the inputed create account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM for that account.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Override
	public Map createAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService createAccount");
 		Map map = new HashMap();
 		Connection con = null;
 
		try {
			//persist data into target system
			executeInputQuery(configData, data, "CreateAccountJSON");
			//retrieve data from target and send it back to SSM 
			String query = "Select * from accounts where name = ? ";
			con = getConnection(data);
     		JSONObject jsonObject = new JSONObject(data.get("ReconcileJSON").toString());
    		Map<String, Object> dataForMap = jsonObject.toMap();
			Map<String, Object> mapperMap = (Map<String, Object>) dataForMap.get("mapper");
			String accountName = data.get("ACCOUNTNAME").toString();
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
	 * to update the account in target system for the inputed update connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM for that account.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
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
			// retrieve data from target and send it back to SSM 
			/** set metadata **/
			//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService updateAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to lock the account in target system for the inputed lock account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM for that account.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception
	 */

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map lockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService lockAccount");
		Map map = new HashMap();
		try {
		    executeInputQuery(configData, data, "LockAccountJSON");
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
		    logger.debug("End DatabaseConnectorService lockAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to disable the account in target system for the inputed disable connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM for that account.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception		
	 */

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map disableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		logger.debug("Enter DatabaseConnectorService disableAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "DisableAccountJSON");
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService disableAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
		}
	/**
	 * to unlock the account in target system for the inputed unlock connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM for that account.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception		 
	 */

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map unLockAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.info("Enter DatabaseConnectorService unLockAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "unLockAccountJSON");
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.info("End DatabaseConnectorService unLockAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return null;
	}
	/**
	 * to enable the account in target system for the inputed enable account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map enableAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService enableAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "EnableAccountJSON");
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService enableAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to remove the account in target system for the inputed remove account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
	 * @throws ConnectorException the connector exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Override
	public Map removeAccount(Map<String, Object> configData, Map<String, Object> data) throws ConnectorException {

		logger.debug("Enter DatabaseConnectorService removeAccount");
		Map map = new HashMap();
		try {
			executeInputQuery(configData, data, "RemoveAccountJSON");
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService removeAccount");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;

	}
	/**
	 * to add access to the account in target system for the inputed addAccess to account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
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
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService AddAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}

	/**
	 * to remove the account access in target system for the inputed remove account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ACCOUNT.COLUMNNAME1=XXXX, ACCOUNT.COLUMNNAME2=XXXX, ACCOUNT.COLUMNNAME3=XXXX}
	 * 
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
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
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService RemoveAccessToAccountJSON");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to update the entitlement in target system for the inputed account connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ENTITLEMENT.COLUMNNAME1=XXXX, ENTITLEMENT.COLUMNNAME2=XXXX, ENTITLEMENT.COLUMNNAME3=XXXX}
	 * 
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection  
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
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
	 * to create the entitlement in target system for the inputed create entitlement connection attributes of connection configuration in SSM
	 * provisioningData sample format: {ENTITLEMENT.COLUMNNAME1=XXXX, ENTITLEMENT.COLUMNNAME2=XXXX, ENTITLEMENT.COLUMNNAME3=XXXX}
	 * 
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return Map which consists of metadata to be updated in SSM (metadata built with help of reconcile json mapping).
	           Metadata is nothing but set of properties to be updated in SSM.
	           Example:SID need to be updated in SSM.Match the corresponding column of SSM and set data to Map
	             map.put("provisioningData", **build map with metadata**);
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
		    // retrieve data from target and send it back to SSM 
		 	/** set metadata **/
		 	//map.put("provisioningData",metadata);
			logger.debug("End DatabaseConnectorService createEntitlement");

		} catch (Exception e) {
			throw new ConnectorException(e);
		}
		return map;
	}
	/**
	 * to update the user in target system for the inputed update user connection attributes of connection configuration in SSM
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection  
	 * @return the integer
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
	 * to terminate the account in target system for the inputed terminate account connection attributes of connection configuration in SSM
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection  
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
	 * to change the password in target system for the inputed change password connection attributes of connection configuration in SSM
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
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
	 * to create the user in target system for the inputed create user connection attributes of connection configuration in SSM
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
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
	 * to validate credentials for the inputed crednetials connection attributes of connection configuration in SSM
	 *
	 * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection  
	 * @return the boolean true or false
	 * @throws ConnectorException the connector exception
	 */
	@Override
	public Boolean validateCredentials(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}
	/**
	 * to get the summary of number of records for the given input object such as accounts.It provides number of accounts,users etc
	 *
     * @param configData the config data for target connection information and other system configuration attributes such as version,status threshold
	 * @param data the Input data for the data objects such as users,account etc from connection 
	 * @return the summary map with object and count as key ,value 
	 */
	@Override
	public Map<String, Object> getSummary(Map<String, Object> configData, Map<String, Object> data) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("Account", 10);
		return map;
	}

	/**
	 * Read database properties for batch size
	 * 
	 * @return
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
	 * 
	 * @param configData
	 * @param data
	 * @param key
	 * @return
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
	 * set properties from target system
	 * @param data
	 * @param con
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
	 * 
	 * @param con
	 * @param mapperMap
	 * @param query
	 * @return
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
	 * 
	 * @param con
	 * @param mapperMap
	 * @param query
	 * @param name
	 * @return
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
	 * 
	 * @param configData
	 * @param data
	 * @return
	 * @throws ConnectorException
	 */
	private Map<String, List<Map<String, Object>>> accountReconcile(Map<String, Object> configData,
			Map<String, Object> data) throws ConnectorException {
		logger.debug("Enter DatabaseConnectorService accountReconcile" + data);

		Map<String, List<Map<String, Object>>> accountMap = new HashMap<String, List<Map<String, Object>>>();
		try {
			logger.debug("dataFromEcm" + data);
			accountMap = processAccountReconcile(configData, data,
					Long.valueOf(data.get("endpointId").toString()));

			logger.info("End DatabaseConnectorService accountReconcile");

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
			throw new ConnectorException(e);
		}
		return accountMap;
	}
	/**
	 * 
	 * @param configData1
	 * @param data
	 * @param endPointId
	 * @return  Map
	 */
	private Map<String, List<Map<String, Object>>> processAccountReconcile(Map<String, Object> configData1,
			Map<String, Object> data, Long endPointId) {
	 
		JSONObject jsonObject = new JSONObject(data.get("ReconcileJSON").toString());
		Map<String, Object> tempdata = jsonObject.toMap();
		Connection con = null;
		Map<String, List<Map<String, Object>>> dataMap = new HashMap<String, List<Map<String, Object>>>();

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

			return dataMap;

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
	 *  set config data
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

	@Override
	public Map firefighterIdGrantAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map firefighterIdInstanceGrantAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map firefighterIdInstanceRevokeAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map firefighterIdRevokeAccess(Map<String, Object> configData, Map<String, Object> data)
			throws ConnectorException {
		// TODO Auto-generated method stub
		return null;
	}

 

}
