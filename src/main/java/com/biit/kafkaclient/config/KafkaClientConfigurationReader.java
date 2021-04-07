package com.biit.kafkaclient.config;

import com.biit.kafkaclient.logger.KafkaClientLogger;
import com.biit.utils.configuration.ConfigurationReader;
import com.biit.utils.configuration.PropertiesSourceFile;
import com.biit.utils.configuration.SystemVariablePropertiesSourceFile;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;
import java.util.Random;

public class KafkaClientConfigurationReader extends ConfigurationReader {

	private static final String SYSTEM_VARIABLE_SETTINGS_FILE = "KAFKA_SETTINGS_FILE";
	private static final String SYSTEM_VARIABLE_CONFIG = "KAFKA_CONFIG";
	private String configFile = "settings.conf";

	private static final String KEY_DESERIALIZER = "key.deserializer";

	private static KafkaClientConfigurationReader instance;

	private KafkaClientConfigurationReader() {
		super();
		//Config file can be modified using Environment variables.
		addProperty(KEY_DESERIALIZER, "asd");
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
		//result.putAll(super.getAllProperties());
		addProperty(KEY_DESERIALIZER, "org.apache.kafka.common.serialization.StringDeserializer");
		result.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "ID" + Math.abs(new Random().nextLong()));
		result.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		result.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		result.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
		result.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		result.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		return result;
	}

}
