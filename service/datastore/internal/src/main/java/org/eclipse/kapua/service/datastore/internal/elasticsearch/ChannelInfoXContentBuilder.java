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
import java.nio.charset.StandardCharsets;
import java.util.Date;

import org.eclipse.kapua.service.datastore.internal.model.ChannelInfoImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoCreator;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.hash.Hashing;

public class ChannelInfoXContentBuilder
{

    @SuppressWarnings("unused")
    private static final Logger   s_logger = LoggerFactory.getLogger(ChannelInfoXContentBuilder.class);

    private String                channelId;
    private XContentBuilder       channelBuilder;

    private void init()
    {
        channelId = null;
        channelBuilder = null;
    }

    private static String getHashCode(String accountName, String clientId, String channel)
    {
        byte[] hashCode = Hashing.sha256()
                                 .hashString(String.format("%s/%s/%s", accountName, clientId, channel), StandardCharsets.UTF_8)
                                 .asBytes();

        return Base64.encodeBytes(hashCode);
        // return aString;
    }

    private static String getChannelKey(String accountName, String clientId, String channel)
    {
        return getHashCode(accountName, clientId, channel);
    }

    private XContentBuilder build(String semChannel, String msgId, Date msgTimestamp, String clientId, String account)
    	throws EsDocumentBuilderException
    {
    	try {
	        XContentBuilder builder = XContentFactory.jsonBuilder()
             .startObject()
                 .field(EsSchema.CHANNEL_NAME, semChannel)
                 .field(EsSchema.CHANNEL_TIMESTAMP, msgTimestamp)
                 .field(EsSchema.CHANNEL_CLIENT_ID, clientId)
                 .field(EsSchema.CHANNEL_ACCOUNT, account)
                 .field(EsSchema.CHANNEL_MESSAGE_ID, msgId)
             .endObject();
	
	        return builder;
    	} catch (IOException e) {
	    	throw new EsDocumentBuilderException(String.format("Unable to build channel info document"), e);
    	}
    }
    
    private static String getOrCreateId(StorableId id, String accountName, String clientId, String channel)
    {
		if (id == null) {
			return getChannelKey(accountName, clientId, channel);
		}
		else
			return id.toString();    	
    }

    public static String getOrCreateId(StorableId id, ChannelInfoCreator channelInfoCreator)
    {
    	return getOrCreateId(id,
    						 channelInfoCreator.getAccount(),
    						 channelInfoCreator.getClientId(),
    						 channelInfoCreator.getChannel());
    }

    public static String getOrCreateId(StorableId id, ChannelInfo channelInfo)
    {
    	return getOrCreateId(id,
    						 channelInfo.getAccount(),
    						 channelInfo.getClientId(),
    						 channelInfo.getChannel());
    }
   
    public ChannelInfoXContentBuilder clear()
    {
        this.init();
        return this;
    }
    
    public ChannelInfoXContentBuilder build(ChannelInfoCreator channelInfoCreator) 
    		throws EsDocumentBuilderException
    {
    	StorableId id = new StorableIdImpl(getOrCreateId(null, channelInfoCreator.getAccount(),
    														   channelInfoCreator.getClientId(),
    														   channelInfoCreator.getChannel()));
    	ChannelInfoImpl channelInfo = new ChannelInfoImpl(channelInfoCreator.getAccount(), id);
    	channelInfo.setClientId(channelInfoCreator.getClientId());
    	channelInfo.setChannel(channelInfoCreator.getChannel());
    	channelInfo.setLastMessageId(channelInfoCreator.getLastMessageId());
    	channelInfo.setLastMessageTimestamp(channelInfoCreator.getLastMessageTimestamp());

    	return this.build(channelInfo);
    }
    

    // TODO move to a dedicated EsChannelBuilder Class
    public ChannelInfoXContentBuilder build(ChannelInfo channelInfo) 
    		throws EsDocumentBuilderException
    {
    	String account = channelInfo.getAccount();
    	String clientId = channelInfo.getClientId();
        String channel = channelInfo.getChannel();
        
        StorableId msgId = channelInfo.getLastMessageId();
        Date msgTimestamp = channelInfo.getLastMessageTimestamp();
        
        XContentBuilder channelBuilder;
		channelBuilder = this.build(channel, msgId.toString(), msgTimestamp, clientId, account);
		
        this.setChannelId(getOrCreateId(channelInfo.getId(), channelInfo.getAccount(),
        													 channelInfo.getClientId(),
        													 channelInfo.getChannel()));
        this.setBuilder(channelBuilder);
        return this;
    }

    public String getChannelId()
    {
        return channelId;
    }

    private void setChannelId(String esChannelId)
    {
        this.channelId = esChannelId;
    }

    public XContentBuilder getBuilder()
    {
        return channelBuilder;
    }

    private void setBuilder(XContentBuilder esChannel)
    {
        this.channelBuilder = esChannel;
    }
}
