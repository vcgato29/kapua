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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.eclipse.kapua.message.KapuaMessage;
import org.eclipse.kapua.message.KapuaPayload;
import org.eclipse.kapua.message.KapuaPosition;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageXContentBuilder
{

    @SuppressWarnings("unused")
    private static final Logger   s_logger = LoggerFactory.getLogger(MessageXContentBuilder.class);

    private StorableIdImpl        messageId;
	private String				  accountName;
    private String				  clientId;
    private String				  channel;
    private String[]			  channelParts;
    private Date				  timestamp;
    private Date				  indexedOn;
    private Date				  receivedOn;
    private Date				  collectedOn;
    private XContentBuilder       messageBuilder;
    
    private Map<String, EsMetric> metricMappings;

    private void init()
    {

        messageId = null;
        messageBuilder = null;
        metricMappings = null;
    }

    private XContentBuilder build(KapuaMessage<?,?> message, String messageId,
                                              Date timestamp, Date indexedOn, Date receivedOn)
        throws EsDocumentBuilderException
    {
    	try {
	    	String accountIdStr = message.getScopeId() == null ? null : message.getScopeId().toCompactId();
	    	String deviceIdStr = message.getDeviceId() == null ? null : message.getDeviceId().toCompactId();
	    	
	        XContentBuilder messageBuilder = XContentFactory.jsonBuilder()
	                                                        .startObject()
	                                                        .field(EsSchema.MESSAGE_TIMESTAMP, timestamp)
	                                                        .field(EsSchema.MESSAGE_RECEIVED_ON, receivedOn) // TODO Which field ??
	                                                        .field(EsSchema.MESSAGE_IP_ADDRESS, "127.0.0.1")
	                                                        .field(EsSchema.MESSAGE_ACCOUNT_ID, accountIdStr)
	                                                        .field(EsSchema.MESSAGE_ACCOUNT, this.getAccountName())
	                                                        .field(EsSchema.MESSAGE_DEVICE_ID,deviceIdStr)
	                                                        .field(EsSchema.MESSAGE_CLIENT_ID, this.getClientId())
	                                                        .field(EsSchema.MESSAGE_CHANNEL, this.getChannel())
	                                                        .field(EsSchema.MESSAGE_CHANNEL_PARTS, this.getChannelParts());
	
	        KapuaPayload payload = message.getPayload();
	        if (payload == null) {
	            messageBuilder.endObject();
	            return messageBuilder;
	        }
	
			this.setCollectedOn(message.getCapturedOn());
	        messageBuilder.field(EsSchema.MESSAGE_COLLECTED_ON, message.getCapturedOn());
	
	        KapuaPosition kapuaPosition = message.getPosition();
	        if (kapuaPosition != null) {
	
	        	Map<String, Object> location = null;
	        	if (kapuaPosition.getLongitude() != null && kapuaPosition.getLatitude() != null) {
		            location = new HashMap<String, Object>();
		            location.put("lon", kapuaPosition.getLongitude());
		            location.put("lat", kapuaPosition.getLatitude());
	        	}
	
	            Map<String, Object> position = new HashMap<String, Object>();
	            position.put(EsSchema.MESSAGE_POS_LOCATION, location);
	            position.put(EsSchema.MESSAGE_POS_ALT, kapuaPosition.getAltitude());
	            position.put(EsSchema.MESSAGE_POS_PRECISION, kapuaPosition.getPrecision());
	            position.put(EsSchema.MESSAGE_POS_HEADING, kapuaPosition.getHeading());
	            position.put(EsSchema.MESSAGE_POS_SPEED, kapuaPosition.getSpeed());
	            position.put(EsSchema.MESSAGE_POS_TIMESTAMP, kapuaPosition.getTimestamp());
	            position.put(EsSchema.MESSAGE_POS_SATELLITES, kapuaPosition.getSatellites());
	            position.put(EsSchema.MESSAGE_POS_STATUS, kapuaPosition.getStatus());
	            messageBuilder.field(EsSchema.MESSAGE_POSITION, position);
	        }
	
	        messageBuilder.field(EsSchema.MESSAGE_BODY, payload.getBody());
	
	        Map<String, EsMetric> metricMappings = new HashMap<String, EsMetric>();
	
	        Map<String, Object> kapuaMetrics = payload.getProperties();
	        if (kapuaMetrics != null) {
	
	            Map<String, Object> metrics = new HashMap<String, Object>();
	            String[] metricNames = kapuaMetrics.keySet().toArray(new String[] {});
	            for (String kapuaMetricName : metricNames) {
	
	                Object metricValue = kapuaMetrics.get(kapuaMetricName);

	                // Sanitize field names: '.' is not allowed
	                String esMetricName = EsUtils.normalizeMetricName(kapuaMetricName);
	                String esType = EsUtils.getEsTypeFromValue(metricValue);
	                String esTypeAcronim = EsUtils.getEsTypeAcronym(esType);	                
	                EsMetric esMetric = new EsMetric();
	                esMetric.setName(esMetricName);
	                esMetric.setType(esType);	                
	
					Map<String, Object> field = new HashMap<String, Object>();
					field.put(esTypeAcronim, metricValue);
					metrics.put(esMetricName, field);
	
	                // each metric is potentially a dynamic field so report it a new mapping
					String mappedName = EsUtils.getMetricValueQualifier(esMetricName, esType);					
	                metricMappings.put(mappedName, esMetric);
	            }
	            messageBuilder.field(EsSchema.MESSAGE_METRICS, metrics);
	        }
	
	        messageBuilder.endObject();
	
	        this.setMetricMappings(metricMappings);
	        return messageBuilder;
    	} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build message document"), e);
    	}
    }

    public MessageXContentBuilder clear()
    {
        this.init();
        return this;
    }
    
    public MessageXContentBuilder build(String account, String clientId, KapuaMessage<?,?> message, Date indexedOn, Date receivedOn) 
    		throws EsDocumentBuilderException
    {
        StorableId messageId;
		UUID uuid = UUID.randomUUID();
		messageId = new StorableIdImpl(uuid.toString());
		
        this.setAccountName(account);
        this.setClientId(clientId);
        
        List<String> parts = message.getChannel().getSemanticParts();
        this.setChannel(DatastoreChannel.getChannel(parts));
        this.setChannelParts(parts.toArray(new String[] {}));

        XContentBuilder messageBuilder;
		messageBuilder = this.build(message, messageId.toString(),
		                                        indexedOn, indexedOn, receivedOn);

        this.setTimestamp(indexedOn);
        this.setIndexedOn(indexedOn);
        this.setReceivedOn(receivedOn);
        this.setMessageId(messageId);
        this.setBuilder(messageBuilder);
        return this;
    }

    public StorableId getMessageId()
    {
        return messageId;
    }

    private void setMessageId(StorableId esMessageId)
    {
        this.messageId = (StorableIdImpl) esMessageId;
    }

    public String getAccountName() {
		return accountName;
	}

	public void setAccountName(String accountName) {
		this.accountName = accountName;
	}

    public String getClientId() {
		return clientId;
	}

	private void setClientId(String clientId) {
		this.clientId = clientId;
	}

	public String getChannel() {
		return channel;
	}

	private void setChannel(String channel) {
		this.channel = channel;
	}

	public String[] getChannelParts() {
		return channelParts;
	}

	private void setChannelParts(String[] channelParts) {
		this.channelParts = channelParts;
	}

	public Date getTimestamp() {
		return timestamp;
	}

	private void setTimestamp(Date timestamp) {
		this.timestamp = timestamp;
	}

	public Date getIndexedOn() {
		return indexedOn;
	}

	private void setIndexedOn(Date indexedOn) {
		this.indexedOn = indexedOn;
	}

	public Date getReceivedOn() {
		return receivedOn;
	}

	private void setReceivedOn(Date receivedOn) {
		this.receivedOn = receivedOn;
	}

	public Date getCollectedOn() {
		return collectedOn;
	}

	private void setCollectedOn(Date collectedOn) {
		this.collectedOn = collectedOn;
	}

	public XContentBuilder getBuilder()
    {
        return messageBuilder;
    }

    private void setBuilder(XContentBuilder esMessage)
    {
        this.messageBuilder = esMessage;
    }

    public Map<String, EsMetric> getMetricMappings()
    {
        return metricMappings;
    }

    private void setMetricMappings(Map<String, EsMetric> metricMappings)
    {
        this.metricMappings = metricMappings;
    }
}
