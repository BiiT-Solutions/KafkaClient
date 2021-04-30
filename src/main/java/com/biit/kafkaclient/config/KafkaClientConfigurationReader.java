package com.biit.kafkaclient.config;

import com.biit.kafkaclient.logger.KafkaClientLogger;
import com.biit.utils.configuration.ConfigurationReader;
import com.biit.utils.configuration.PropertiesSourceFile;
import com.biit.utils.configuration.SystemVariablePropertiesSourceFile;
import com.biit.utils.configuration.exceptions.PropertyNotFoundException;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class KafkaClientConfigurationReader extends ConfigurationReader {

	private static final String SYSTEM_VARIABLE_SETTINGS_FILE = "KAFKA_SETTINGS_FILE";
	private static final String SYSTEM_VARIABLE_CONFIG = "KAFKA_CONFIG";
	private String configFile = "settings.conf";


	private static final String KEY_DESERIALIZER_CLASS_CONFIG = "key.deserializer";
	private static final String VALUE_DESERIALIZER_CLASS_CONFIG = "value.deserializer";
	private static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";
	private static final String KEY_SERIALIZER_CLASS_CONFIG = "key.serializer";
	private static final String VALUE_SERIALIZER_CLASS_CONFIG = "value.serializer";

	// Default
	private static final String DEFAULT_KEY_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";
	private static final String DEFAULT_VALUE_DESERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringDeserializer";
	private static final String DEFAULT_BOOTSTRAP_SERVERS_CONFIG = "kafka:9092";
	private static final String DEFAULT_KEY_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";
	private static final String DEFAULT_VALUE_SERIALIZER_CLASS_CONFIG = "org.apache.kafka.common.serialization.StringSerializer";

	private static KafkaClientConfigurationReader instance;

	private KafkaClientConfigurationReader() {
		super();
		//Config file can be modified using Environment variables.

		addProperty(KEY_DESERIALIZER_CLASS_CONFIG, DEFAULT_KEY_DESERIALIZER_CLASS_CONFIG);
		addProperty(VALUE_DESERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_DESERIALIZER_CLASS_CONFIG);
		addProperty(BOOTSTRAP_SERVERS_CONFIG, DEFAULT_BOOTSTRAP_SERVERS_CONFIG);
		addProperty(KEY_SERIALIZER_CLASS_CONFIG, DEFAULT_KEY_SERIALIZER_CLASS_CONFIG);
		addProperty(VALUE_SERIALIZER_CLASS_CONFIG, DEFAULT_VALUE_SERIALIZER_CLASS_CONFIG);

		String settingsFileName=null;
		try{
			if(SYSTEM_VARIABLE_SETTINGS_FILE != null){
				settingsFileName = System.getenv(SYSTEM_VARIABLE_SETTINGS_FILE);
			}
		}catch (SecurityException e){
			KafkaClientLogger.errorMessage(getClass().getName(),"REQUIRED SYSTEM PERMISSIONS: Has been not possible retrieve environment variable '" +
					SYSTEM_VARIABLE_SETTINGS_FILE+ "' due to system permissions. I'm gonna carry on with default settings file name");
		}

		if(settingsFileName!=null){
			if(!settingsFileName.isEmpty()){
				configFile = settingsFileName;
				KafkaClientLogger.debug(getClass().getName(), "Esper settings filename has been set to '" + configFile + "'");
			}else {
                KafkaClientLogger.debug(getClass().getName(), "'" + SYSTEM_VARIABLE_SETTINGS_FILE + " ' is empty");
            }
		}else{
            KafkaClientLogger.debug(getClass().getName(), "'" + SYSTEM_VARIABLE_SETTINGS_FILE + " ' not defined");
        }

		final PropertiesSourceFile sourceFile = new PropertiesSourceFile(configFile);
		sourceFile.addFileModifiedListeners(path -> {
			KafkaClientLogger.info(this.getClass().getName(), "Jar settings file changed");
			readConfigurations();
		});
		addPropertiesSource(sourceFile);

		final SystemVariablePropertiesSourceFile systemSourceFile = new SystemVariablePropertiesSourceFile(SYSTEM_VARIABLE_CONFIG, configFile);
		systemSourceFile.addFileModifiedListeners(path -> {
			KafkaClientLogger.info(this.getClass().getName(), "System variable settings file changed");
			readConfigurations();
		});
		addPropertiesSource(systemSourceFile);
		// initializeAllProperties();
		readConfigurations();
	}

	public static KafkaClientConfigurationReader getInstance() {
		return instance == null ? instance = new KafkaClientConfigurationReader() : instance;
	}

	public Properties getAllPropertiesAsPropertiesClass() {
		Properties result = new Properties();
		result.putAll(super.getAllProperties());

		/*result.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ID" + Math.abs(new Random().nextLong()));
		result.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, getKeyDeserializerClassConfig());
		result.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, getValueDeserializerClassConfig());
		result.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBootstrapServersConfig());
		result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, getKeySerializerClassConfig());
		result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializerClassConfig());*/
		return result;
	}

	private String getPropertyLogException(String propertyId) {
		try {
			return getProperty(propertyId);
		} catch (PropertyNotFoundException e) {
			KafkaClientLogger.errorMessage(this.getClass().getName(), e);
			return null;
		}
	}

	public String getKeyDeserializerClassConfig() {
		return getPropertyLogException(KEY_DESERIALIZER_CLASS_CONFIG);
	}

	public String getValueDeserializerClassConfig() {
		return getPropertyLogException(VALUE_DESERIALIZER_CLASS_CONFIG);
	}

	public String getBootstrapServersConfig() {
		return getPropertyLogException(BOOTSTRAP_SERVERS_CONFIG);
	}

	public String getKeySerializerClassConfig() {
		return getPropertyLogException(KEY_SERIALIZER_CLASS_CONFIG);
	}

	public String getValueSerializerClassConfig() {
		return getPropertyLogException(VALUE_SERIALIZER_CLASS_CONFIG);
	}

}
