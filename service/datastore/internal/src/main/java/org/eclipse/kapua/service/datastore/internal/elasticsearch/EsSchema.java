/*******************************************************************************
 * Copyright (c) 2011, 2017 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *  
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.service.datastore.internal.elasticsearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Pattern;

import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettingKey;
import org.eclipse.kapua.service.datastore.internal.setting.DatastoreSettings;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsRequest;
import org.elasticsearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EsSchema
{

    // TODO move builder methods in a new class to reduce number of lines
    //
    private static final Logger s_logger                        = LoggerFactory.getLogger(EsSchema.class);

    public final static String  MESSAGE_TYPE_NAME               = "message";
    public final static String  MESSAGE_TIMESTAMP               = "timestamp";
    public final static String  MESSAGE_RECEIVED_ON             = "received_on";
    public final static String  MESSAGE_IP_ADDRESS              = "ip_address";
    public final static String  MESSAGE_ACCOUNT_ID              = "account_id";
    public final static String  MESSAGE_ACCOUNT                 = "account";
    public final static String  MESSAGE_DEVICE_ID               = "device_id";
    public final static String  MESSAGE_CLIENT_ID               = "client_id";
    public final static String  MESSAGE_CHANNEL               	= "channel";
    public final static String  MESSAGE_CHANNEL_PARTS           = "channel_parts";
    public final static String  MESSAGE_COLLECTED_ON            = "collected_on";
    public final static String  MESSAGE_POSITION                = "position";
    public final static String  MESSAGE_POS_LOCATION            = "location";
    public final static String  MESSAGE_POS_LOCATION_FULL       = "position.location";
    public final static String  MESSAGE_POS_ALT                 = "alt";
    public final static String  MESSAGE_POS_ALT_FULL            = "position.alt";
    public final static String  MESSAGE_POS_PRECISION           = "precision";
    public final static String  MESSAGE_POS_PRECISION_FULL      = "position.precision";
    public final static String  MESSAGE_POS_HEADING             = "heading";
    public final static String  MESSAGE_POS_HEADING_FULL        = "position.heading";
    public final static String  MESSAGE_POS_SPEED               = "speed";
    public final static String  MESSAGE_POS_SPEED_FULL          = "position.speed";
    public final static String  MESSAGE_POS_TIMESTAMP           = "timestamp";
    public final static String  MESSAGE_POS_TIMESTAMP_FULL      = "position.timestamp";
    public final static String  MESSAGE_POS_SATELLITES          = "satellites";
    public final static String  MESSAGE_POS_SATELLITES_FULL     = "position.satellites";
    public final static String  MESSAGE_POS_STATUS              = "status";
    public final static String  MESSAGE_POS_STATUS_FULL         = "position.status";
    public final static String  MESSAGE_METRICS                 = "metrics";
    public final static String  MESSAGE_BODY                    = "body";

    public final static String  CHANNEL_TYPE_NAME               = "channel";
    public final static String  CHANNEL_NAME                    = "channel";
    public final static String  CHANNEL_CLIENT_ID               = "client_id";
    public final static String  CHANNEL_ACCOUNT                 = "account";
    public final static String  CHANNEL_TIMESTAMP               = "timestamp";
    public final static String  CHANNEL_MESSAGE_ID              = "message_id";

    public final static String  METRIC_TYPE_NAME            = "metric";
    public final static String  METRIC_CHANNEL              = "channel";
    public final static String  METRIC_CLIENT_ID            = "client_id";
    public final static String  METRIC_ACCOUNT              = "account";
    public final static String  METRIC_MTR                  = "metric";
    public final static String  METRIC_MTR_NAME             = "name";
    public final static String  METRIC_MTR_NAME_FULL        = "metric.name";
    public final static String  METRIC_MTR_TYPE             = "type";
    public final static String  METRIC_MTR_TYPE_FULL        = "metric.type";
    public final static String  METRIC_MTR_VALUE            = "value";
    public final static String  METRIC_MTR_VALUE_FULL       = "metric.value";
    public final static String  METRIC_MTR_TIMESTAMP        = "timestamp";
    public final static String  METRIC_MTR_TIMESTAMP_FULL   = "metric.timestamp";
    public final static String  METRIC_MTR_MSG_ID           = "message_id";
    public final static String  METRIC_MTR_MSG_ID_FULL      = "metric.message_id";

    public final static String  CLIENT_TYPE_NAME            = "client";
    public final static String  CLIENT_ID                   = "client_id";
    public final static String  CLIENT_ACCOUNT              = "account";
    public final static String  CLIENT_TIMESTAMP            = "timestamp";
    public final static String  CLIENT_MESSAGE_ID           = "message_id";

    public class Metadata
    {

        // Info fields does not change within the same account name
        private String                dataIndexName;
        private String                kapuaIndexName;
        //

        // Custom mappings can only increase within the same account
        // No removal of existing cached mappings or changes in the
        // existing mappings.
        private Map<String, EsMetric> messageMappingsCache;
        //

        private Map<String, EsMetric> getMessageMappingsCache()
        {
            return messageMappingsCache;
        }

        public Metadata()
        {
            messageMappingsCache = new HashMap<String, EsMetric>(100);
        }

        public String getDataIndexName()
        {
            return this.dataIndexName;
        }

        public String getKapuaIndexName()
        {
            return this.kapuaIndexName;
        }
    }

    private Map<String, Metadata> schemaCache;
    private Object                schemaCacheSync;
    private Object                mappingsSync;

    private XContentBuilder getIndexSettings()
    	throws EsDocumentBuilderException
    {
    	try {
	        DatastoreSettings config = DatastoreSettings.getInstance();
	        String idxRefreshInterval = String.format("%ss", config.getLong(DatastoreSettingKey.ELASTICSEARCH_IDX_REFRESH_INTERVAL));
	
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	        .startObject()
	            .startObject("index")
	                .field("refresh_interval", idxRefreshInterval)
	            .endObject()
	        .endObject();
	
	        return builder;
    	} catch (IOException e) {
    		throw new EsDocumentBuilderException(String.format("Unable to build settings for index"), e);
    	}
    }

    private XContentBuilder getClientTypeBuilder(boolean allEnable, boolean sourceEnable)
    	throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	         .startObject()
	             .startObject(CLIENT_TYPE_NAME)
	                 .startObject("_source")
	                     .field("enabled", sourceEnable)
	                 .endObject()
	                 .startObject("_all")
	                     .field("enabled", allEnable)
	                 .endObject()
	                 .startObject("properties")
	                     .startObject(CLIENT_ID)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(CLIENT_TIMESTAMP)
	                         .field("type", "date")
	                         // .field("format", "basic_date_time||basic_date_time_no_millis||epoch_millis")
	                     .endObject()
	                     .startObject(CLIENT_ACCOUNT)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(CLIENT_MESSAGE_ID)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                  .endObject() // End Of Properties
	             .endObject() // End of type
	         .endObject();
	
	        return builder;
    	} catch (IOException e) {
        	throw new EsDocumentBuilderException(String.format("Unable to build type mappings for type %s", CLIENT_TYPE_NAME), e);
        }
    }

    private XContentBuilder getMetricTypeBuilder(boolean allEnable, boolean sourceEnable)
    	throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	         .startObject()
	             .startObject(METRIC_TYPE_NAME)
	                 .startObject("_source")
	                     .field("enabled", sourceEnable)
	                 .endObject()
	                 .startObject("_all")
	                     .field("enabled", allEnable)
	                 .endObject()
	                 .startObject("properties")
	                     .startObject(METRIC_ACCOUNT)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(METRIC_CLIENT_ID)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(METRIC_CHANNEL)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(METRIC_MTR)
	                         .field("type", "object")
	                         .field("enabled", true)
	                         .field("dynamic", false)
	                         .field("include_in_all", false)
	                         .startObject("properties")
	                             .startObject(METRIC_MTR_NAME)
	                                 .field("type", "string")
	                                 .field("index", "not_analyzed")
	                             .endObject()
	                             .startObject(METRIC_MTR_TYPE)
	                                 .field("type", "string")
	                                 .field("index", "not_analyzed")
	                             .endObject()
	                             .startObject(METRIC_MTR_VALUE)
	                                 .field("type", "string")
	                                 .field("index", "not_analyzed")
	                             .endObject()
	                             .startObject(METRIC_MTR_TIMESTAMP)
	                                 .field("type", "date")
	                             .endObject()
	                             .startObject(METRIC_MTR_MSG_ID)
	                                 .field("type", "string")
	                                 .field("index", "not_analyzed")
	                             .endObject()
	                         .endObject() // End of properties
	                     .endObject() // End of metrics
	                 .endObject() // End Of Properties
	             .endObject() // End of type
	         .endObject();
	
	        return builder;
		} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build type mappings for type %s", METRIC_TYPE_NAME), e);
	    }
   }

    private XContentBuilder getChannelTypeBuilder(boolean allEnable, boolean sourceEnable)
    	throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	         .startObject()
	         	.startObject(CHANNEL_TYPE_NAME)
	         		.startObject("_source")
	         			.field("enabled", sourceEnable)
	         		.endObject()
	         		.startObject("_all")
	         			.field("enabled", allEnable)
	         		.endObject()
	         		.startObject("properties")
	         			.startObject(CHANNEL_ACCOUNT)
	         				.field("type", "string")
	         				.field("index", "not_analyzed")
	         			.endObject()
	         			.startObject(CHANNEL_CLIENT_ID)
	         				.field("type", "string")
	         				.field("index", "not_analyzed")
	         			.endObject()
	         			.startObject(CHANNEL_NAME)
	         				.field("type", "string")
	         				.field("index", "not_analyzed")
	         			.endObject()
	         			.startObject(CHANNEL_TIMESTAMP)
	         				.field("type", "date")
	         				// .field("format", "basic_date_time||basic_date_time_no_millis||epoch_millis")
	         			.endObject()
	         			.startObject(CHANNEL_MESSAGE_ID)
	         				.field("type", "string")
	         				.field("index", "not_analyzed")
	         			.endObject()
	         		.endObject() // End Of Properties
	         	.endObject() // End of type
	         .endObject();
	
	        return builder;
    	} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build type mappings for type %s", CHANNEL_TYPE_NAME), e);
    	}
    }

    private XContentBuilder getMessageTypeBuilder(boolean allEnable, boolean sourceEnable)
    	throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	         .startObject()
	             .startObject(MESSAGE_TYPE_NAME)
	                 .startObject("_source")
	                     .field("enabled", sourceEnable)
	                 .endObject()
	                 .startObject("_all")
	                     .field("enabled", allEnable)
	                 .endObject()
	                 .startObject("properties")
	                     .startObject(EsSchema.MESSAGE_TIMESTAMP)
	                         .field("type", "date")
	                         // .field("format", "basic_date_time||basic_date_time_no_millis||epoch_millis")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_RECEIVED_ON)
	                         .field("type", "date")
	                         // .field("format", "basic_date_time||basic_date_time_no_millis||epoch_millis")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_IP_ADDRESS)
	                         .field("type", "ip")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_ACCOUNT_ID)
	                     	.field("type", "string")
	                     	.field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_ACCOUNT)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_DEVICE_ID)
	                     	.field("type", "string")
	                     	.field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_CLIENT_ID)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_CHANNEL)
	                         .field("type", "string")
	                         .field("index", "not_analyzed")
	                     .endObject()
	                     // .startObject(EsSchema.MESSAGE_TOPIC_PARTS)
	                         // .field("type", "string")
	                         // .field("index", "not_analyzed")
	                     // .endObject()
	                     .startObject(EsSchema.MESSAGE_COLLECTED_ON)
	                         .field("type", "date")
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_POSITION)
	                         .field("type", "object")
	                         .field("enabled", true)
	                         .field("dynamic", false)
	                         .field("include_in_all", false)
	                         .startObject("properties")
	                             .startObject(EsSchema.MESSAGE_POS_LOCATION)
	                                 .field("type", "geo_point")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_ALT)
	                                 .field("type", "double")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_PRECISION)
	                                 .field("type", "double")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_HEADING)
	                                 .field("type", "double")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_SPEED)
	                                 .field("type", "double")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_TIMESTAMP)
	                                 .field("type", "date")
	                                 // .field("format", "basic_date_time||basic_date_time_no_millis||epoch_millis")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_SATELLITES)
	                                 .field("type", "integer")
	                             .endObject()
	                             .startObject(EsSchema.MESSAGE_POS_STATUS)
	                                 .field("type", "integer")
	                             .endObject()
	                         .endObject()
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_METRICS)
	                         .field("type", "object")
	                         .field("enabled", true)
	                         .field("dynamic", true)
	                         .field("include_in_all", false)
	                     .endObject()
	                     .startObject(EsSchema.MESSAGE_BODY)
	                         .field("type", "binary")
	                         .field("index", "no")
	                     .endObject()
	                 .endObject() // End Of Properties
	             .endObject();
	
	        return builder;
    	} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build type mappings for type %s", CHANNEL_TYPE_NAME), e);
    	}
    }

    private XContentBuilder getNewMessageMappingsBuilder(Map<String, EsMetric> esMetrics) 
    		throws EsDocumentBuilderException
    {
        final int METRIC_TERM = 0;
        // final int TYPE_TERM = 1;

        if (esMetrics == null)
            return null;

        try {
	        // It is assumed the mappings (key values) are all of the type
	        // metrics.metric_name.type
	        XContentBuilder builder = XContentFactory.jsonBuilder()
	         .startObject()
	         .startObject(MESSAGE_TYPE_NAME)
	         .startObject("properties")
	         .startObject(EsSchema.MESSAGE_METRICS)
	         .startObject("properties");
	
	        // TODO precondition for the loop: there are no two consecutive mappings for the same field with
	        // two different types (field are all different)
	
	        String[] prevKeySplit = new String[] { "", "" };
	        Set<String> keys = esMetrics.keySet();
	        for (String key : keys) {
	
	            EsMetric metric = esMetrics.get(key);
	            String[] keySplit = key.split(Pattern.quote("."));
	
	            if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {
	                if (!prevKeySplit[METRIC_TERM].isEmpty()) {
	                    builder.endObject(); // Previously open properties section
	                    builder.endObject(); // Previously open metric-object section
	                }
	                builder.startObject(metric.getName()); // Start new metric object
	                builder.startObject("properties"); // Start new object properties section
	            }
	
	            if (!keySplit[METRIC_TERM].equals(prevKeySplit[METRIC_TERM])) {
	            	
	            	builder.startObject(EsUtils.getEsTypeAcronym(metric.getType()));	
	                builder.field("type", metric.getType());
	                if (metric.getType().equals("string"))
	                    builder.field("index", "not_analyzed");
	                builder.endObject();
	            }
	
	            prevKeySplit = keySplit;
	        }
	
	        if (keys.size() > 0) {
	            if (!prevKeySplit[METRIC_TERM].isEmpty()) {
	                builder.endObject(); // Previously open properties section
	                builder.endObject(); // Previously open metrics-object section
	            }
	        }
	
	        builder.endObject() // Properties
	               .endObject() // Metrics
	               .endObject() // Properties
	               .endObject() // Type
	               .endObject(); // Root
	
	        return builder;
        } catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build new type mappings for type %s", CHANNEL_TYPE_NAME), e);
        }
    }

    private void initMessageMappings(String indexName, boolean allEnable, boolean sourceEnable)
        throws EsDocumentBuilderException, EsClientUnavailableException
    {

        Client esClient = ElasticsearchClient.getInstance();

        // Check message type mapping
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(indexName);
        GetMappingsResponse mappingsResponse = esClient.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(indexName);
        MappingMetaData metadata = map.get(MESSAGE_TYPE_NAME);
        if (metadata == null) {
            XContentBuilder builder = this.getMessageTypeBuilder(allEnable, sourceEnable);
            esClient.admin().indices().preparePutMapping(indexName).setType(MESSAGE_TYPE_NAME).setSource(builder).execute().actionGet();

            try {
				s_logger.trace("Message mapping created: " + builder.string());
			} catch (IOException e) {
				s_logger.trace("Message mapping created: (content unavailable)");
			}
        }
    }

    private void initTopicMappings(String indexName, boolean allEnable, boolean sourceEnable)
        throws EsDocumentBuilderException, EsClientUnavailableException
    {

        Client esClient = ElasticsearchClient.getInstance();

        // Check message type mapping
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(indexName);
        GetMappingsResponse mappingsResponse = esClient.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(indexName);
        MappingMetaData metadata = map.get(CHANNEL_TYPE_NAME);
        if (metadata == null) {
            XContentBuilder builder = this.getChannelTypeBuilder(allEnable, sourceEnable);
            esClient.admin().indices().preparePutMapping(indexName).setType(CHANNEL_TYPE_NAME).setSource(builder).execute().actionGet();

            try {
				s_logger.trace("Topic mapping created: " + builder.string());
			} catch (IOException e) {
				s_logger.trace("Topic mapping created: (content unavailable)");
			}
        }
    }

    private void initMetricMappings(String indexName, boolean allEnable, boolean sourceEnable)
        throws EsDocumentBuilderException, EsClientUnavailableException
    {

        Client esClient = ElasticsearchClient.getInstance();

        // Check message type mapping
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(indexName);
        GetMappingsResponse mappingsResponse = esClient.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(indexName);
        MappingMetaData metadata = map.get(METRIC_TYPE_NAME);
        if (metadata == null) {
            XContentBuilder builder = this.getMetricTypeBuilder(allEnable, sourceEnable);
            esClient.admin().indices().preparePutMapping(indexName).setType(METRIC_TYPE_NAME).setSource(builder).execute().actionGet();
 
            try {
				s_logger.trace("Topic_metric mapping created: " + builder.string());
			} catch (IOException e) {
				s_logger.trace("Topic_metric mapping created: (content unavailable)");
			}
        }
    }

    private void initClientMappings(String indexName, boolean allEnable, boolean sourceEnable)
        throws EsDocumentBuilderException, EsClientUnavailableException
    {

        Client esClient = ElasticsearchClient.getInstance();

        // Check message type mapping
        GetMappingsRequest mappingsRequest = new GetMappingsRequest().indices(indexName);
        GetMappingsResponse mappingsResponse = esClient.admin().indices().getMappings(mappingsRequest).actionGet();
        ImmutableOpenMap<String, ImmutableOpenMap<String, MappingMetaData>> mappings = mappingsResponse.getMappings();
        ImmutableOpenMap<String, MappingMetaData> map = mappings.get(indexName);
        MappingMetaData metadata = map.get(CLIENT_TYPE_NAME);
        if (metadata == null) {
            XContentBuilder builder = this.getClientTypeBuilder(allEnable, sourceEnable);
            esClient.admin().indices().preparePutMapping(indexName).setType(CLIENT_TYPE_NAME).setSource(builder).execute().actionGet();
            
            try {
				s_logger.trace("Asset mapping created: " + builder.string());
			} catch (IOException e) {
				s_logger.trace("Asset mapping created: (content unavailable)");
			}
        }
    }

    private Map<String, EsMetric> getMessageMappingDiffs(Metadata currentMetadata, Map<String, EsMetric> esMetrics)
    {

        if (esMetrics == null || esMetrics.size() == 0)
            return null;

        Entry<String, EsMetric> el;
        Map<String, EsMetric> diffs = null;
        Iterator<Entry<String, EsMetric>> iter = esMetrics.entrySet().iterator();
        while (iter.hasNext()) {

            el = iter.next();
            if (!currentMetadata.getMessageMappingsCache().containsKey(el.getKey())) {

                if (diffs == null)
                    diffs = new HashMap<String, EsMetric>(100);

                currentMetadata.getMessageMappingsCache().put(el.getKey(), el.getValue());
                diffs.put(el.getKey(), el.getValue());
            }
        }

        return diffs;
    }

    public EsSchema()
    {
        schemaCache = new HashMap<String, Metadata>();
        schemaCacheSync = new Object();
        mappingsSync = new Object();
    }

    public static String getDataIndexName(KapuaId scopeId) 
    {
    	String scopeIdShort = scopeId.toCompactId();
    	return EsUtils.getDataIndexName(scopeIdShort);
    }

    public static String getKapuaIndexName(KapuaId scopeId) 
    {
    	String scopeIdShort = scopeId.toCompactId();
    	return EsUtils.getKapuaIndexName(scopeIdShort);
    }
    
    public Metadata synch(KapuaId scopeId, long time) 
    		throws EsDocumentBuilderException, EsClientUnavailableException
    {
    	String scopeIdShort = scopeId.toCompactId();
        String newIndex = EsUtils.getDataIndexName(scopeIdShort, time);

        synchronized (schemaCacheSync) {
            if (schemaCache.containsKey(newIndex)) {
                Metadata currentMetadata = schemaCache.get(newIndex);
                return currentMetadata;
            }
        }

        s_logger.info("Before entering updating metadata");

        Metadata currentMetadata = null;
        synchronized (mappingsSync) {
            s_logger.info("Entered updating metadata");
            currentMetadata = new Metadata();

            IndicesExistsResponse existsResponse = null;
            Client esClient = ElasticsearchClient.getInstance();

            // Check existence of the data index
            existsResponse = esClient.admin().indices()
                                     .exists(new IndicesExistsRequest(newIndex))
                                     .actionGet();

            boolean indexExists = existsResponse.isExists();
            if (!indexExists) {
                esClient.admin().indices()
                        .prepareCreate(newIndex)
                        .setSettings(this.getIndexSettings())
                        .execute()
                        .actionGet();

                s_logger.info("Data index created: " + newIndex);
            }

            boolean enableAllField = false;
            boolean enableSourceField = true;

            this.initMessageMappings(newIndex, enableAllField, enableSourceField);

            // Check existence of the kapua internal index
            String newKapuaMetadataIdx = EsUtils.getKapuaIndexName(scopeIdShort);
            existsResponse = esClient.admin().indices()
                                     .exists(new IndicesExistsRequest(newKapuaMetadataIdx))
                                     .actionGet();

            indexExists = existsResponse.isExists();
            if (!indexExists) {
                esClient.admin()
                        .indices()
                        .prepareCreate(newKapuaMetadataIdx)
                        .setSettings(this.getIndexSettings())
                        .execute()
                        .actionGet();

                s_logger.info("Metadata index created: " + newKapuaMetadataIdx);

                this.initTopicMappings(newKapuaMetadataIdx, enableAllField, enableSourceField);
                this.initMetricMappings(newKapuaMetadataIdx, enableAllField, enableSourceField);
                this.initClientMappings(newKapuaMetadataIdx, enableAllField, enableSourceField);
            }

            currentMetadata.dataIndexName = newIndex;
            currentMetadata.kapuaIndexName = newKapuaMetadataIdx;
            s_logger.info("Leaving updating metadata");
        }

        synchronized (schemaCacheSync) {
            // Current metadata can only increase the custom mappings
            // other fields does not change within the same account id
            // and custom mappings are not and must not be exposed to
            // outside this class to preserve thread safetyness
            schemaCache.put(newIndex, currentMetadata);
        }

        return currentMetadata;
    }

    public void updateMessageMappings(KapuaId scopeId, long time, Map<String, EsMetric> esMetrics) 
    		throws EsDocumentBuilderException, EsClientUnavailableException
    {
        if (esMetrics.size() == 0)
            return;

        Metadata currentMetadata = null;
        synchronized (schemaCacheSync) {
        	String scopeIdShort = scopeId.toCompactId();
            String newIndex = EsUtils.getDataIndexName(scopeIdShort, time);
            currentMetadata = schemaCache.get(newIndex);
        }

        XContentBuilder builder = null;
        Map<String, EsMetric> diffs = null;

        synchronized (mappingsSync) {

            // Update mappings only if a metric is new (not in cache)
            diffs = this.getMessageMappingDiffs(currentMetadata, esMetrics);
            if (diffs == null || diffs.size() == 0)
                return;

            builder = this.getNewMessageMappingsBuilder(diffs);
        }

        try {
			s_logger.trace("Sending dynamic message mappings: " + builder.string());
		} catch (IOException e) {
		}
        
        Client esClient = ElasticsearchClient.getInstance();
        esClient.admin().indices().preparePutMapping(currentMetadata.dataIndexName)
                .setType(MESSAGE_TYPE_NAME)
                .setSource(builder)
                .execute().actionGet();
    }
}
