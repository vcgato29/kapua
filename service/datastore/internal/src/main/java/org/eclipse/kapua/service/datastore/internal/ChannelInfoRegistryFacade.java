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
package org.eclipse.kapua.service.datastore.internal;

import java.util.ArrayList;

import org.eclipse.kapua.KapuaIllegalArgumentException;
import org.eclipse.kapua.commons.util.ArgumentValidator;
import org.eclipse.kapua.commons.util.KapuaDateUtils;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ChannelInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ChannelInfoXContentBuilder;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ElasticsearchClient;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsClientUnavailableException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsConfigurationException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsDocumentBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsObjectBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsQueryConversionException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema.Metadata;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.LocalServicePlan;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.dao.EsChannelInfoDAO;
import org.eclipse.kapua.service.datastore.internal.model.ChannelInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ChannelInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfoCreator;
import org.eclipse.kapua.service.datastore.model.ChannelInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ChannelInfoQuery;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelInfoRegistryFacade 
{
    // @SuppressWarnings("unused")
    private static final Logger  logger           = LoggerFactory.getLogger(ChannelInfoRegistryFacade.class);

    private final ChannelInfoRegistryMediator mediator;
	private final ConfigurationProvider configProvider;
	private final Object metadataUpdateSync;

	public ChannelInfoRegistryFacade(ConfigurationProvider configProvider, ChannelInfoRegistryMediator mediator)
	{
		this.configProvider = configProvider;
		this.mediator = mediator;
		this.metadataUpdateSync = new Object();
	}

	public StorableId store(KapuaId scopeId, ChannelInfoCreator channelInfoCreator) 
			throws KapuaIllegalArgumentException, 
				   EsDocumentBuilderException, 
				   EsClientUnavailableException, 
				   EsConfigurationException 
	{
		//
		// Argument Validation
		ArgumentValidator.notNull(scopeId, "scopeId");
		ArgumentValidator.notNull(channelInfoCreator, "channelInfoCreator");
		ArgumentValidator.notNull(channelInfoCreator.getChannel(), "channelInfoCreator.getChannel");
		ArgumentValidator.notNull(channelInfoCreator.getLastMessageTimestamp(), "channelInfoCreator.lastMessageTimestamp");
        
        String channelInfoId = ChannelInfoXContentBuilder.getOrCreateId(null, channelInfoCreator);

		// Store channel. Look up channel in the cache, and cache it if it doesn't exist
		if (!DatastoreCacheManager.getInstance().getChannelsCache().get(channelInfoId)) {

			// The code is safe even without the synchronized block
			// Synchronize in order to let the first thread complete its
			// update then the others of the same type will find the cache 
			// updated and skip the update.
			synchronized (this.metadataUpdateSync) 
			{
				if (!DatastoreCacheManager.getInstance().getChannelsCache().get(channelInfoId)) {
					UpdateResponse response = null;
					try 
					{
						Metadata metadata = this.mediator.getMetadata(scopeId, channelInfoCreator.getLastMessageTimestamp().getTime());
						String kapuaIndexName = metadata.getKapuaIndexName();

						response = EsChannelInfoDAO.client(ElasticsearchClient.getInstance())
											 .index(metadata.getKapuaIndexName())
											 .upsert(channelInfoCreator);
						
						channelInfoId = response.getId();
						
						logger.debug(String.format("Upsert on channel succesfully executed [%s.%s, %s]",
									 kapuaIndexName, EsSchema.CHANNEL_TYPE_NAME, channelInfoId));
						
					} catch (DocumentAlreadyExistsException exc) {
						logger.trace(String.format("Upsert failed because channel already exists [%s, %s]",
									 channelInfoId, exc.getMessage()));
					}
					// Update cache if channel update is completed successfully
					DatastoreCacheManager.getInstance().getChannelsCache().put(channelInfoId, true);
				}
			}
		}
		return new StorableIdImpl(channelInfoId);
	}

    public void delete(KapuaId scopeId, StorableId id) 
    		throws KapuaIllegalArgumentException, 
    			   EsClientUnavailableException,
    			   EsConfigurationException, 
    			   EsQueryConversionException, 
    			   EsObjectBuilderException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        //
        // Do the find
        LocalServicePlan accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLive() * KapuaDateUtils.DAY_MILLIS;

        if (!accountServicePlan.getDataStorageEnabled() || ttl == LocalServicePlan.DISABLED) {
            logger.debug("Storage not enabled for account {}, return", scopeId);
            return;
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);

        ChannelInfo channelInfo = this.find(scopeId, id);

        this.mediator.onBeforeChannelInfoDelete(scopeId, channelInfo);

        EsChannelInfoDAO.client(ElasticsearchClient.getInstance())
                  .index(indexName)
                  .deleteById(id.toString());
    }

    public ChannelInfo find(KapuaId scopeId, StorableId id) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsClientUnavailableException, 
    			   EsQueryConversionException, 
    			   EsObjectBuilderException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        ChannelInfoQueryImpl q = new ChannelInfoQueryImpl();
        q.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(EsSchema.CHANNEL_TYPE_NAME, ids));

        ChannelInfoListResult result = this.query(scopeId, q);
        if (result == null || result.size() == 0)
            return null;

        ChannelInfo channelInfo = result.get(0);
        return channelInfo;
    }

    public ChannelInfoListResult query(KapuaId scopeId, ChannelInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsClientUnavailableException, 
    			   EsQueryConversionException, 
    			   EsObjectBuilderException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        LocalServicePlan accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLive() * KapuaDateUtils.DAY_MILLIS;

        if (!accountServicePlan.getDataStorageEnabled() || ttl == LocalServicePlan.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return new ChannelInfoListResultImpl();
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);
        ChannelInfoListResult result = null;
        result = EsChannelInfoDAO.client(ElasticsearchClient.getInstance())
                           .index(indexName)
                           .query(query);

        return result;
    }

    public long count(KapuaId scopeId, ChannelInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsQueryConversionException, 
    			   EsClientUnavailableException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        LocalServicePlan accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLive() * KapuaDateUtils.DAY_MILLIS;

        if (!accountServicePlan.getDataStorageEnabled() || ttl == LocalServicePlan.DISABLED) {
            logger.debug("Storage not enabled for account {}, returning empty result", scopeId);
            return 0;
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);
        long result;
        result = EsChannelInfoDAO.client(ElasticsearchClient.getInstance())
                           .index(indexName)
                           .count(query);

        return result;
    }

    public void delete(KapuaId scopeId, ChannelInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsQueryConversionException, 
    			   EsClientUnavailableException, 
    			   EsConfigurationException, 
    			   EsObjectBuilderException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(query, "query");

        //
        // Do the find
        LocalServicePlan accountServicePlan = this.configProvider.getConfiguration(scopeId);
        long ttl = accountServicePlan.getDataTimeToLive() * KapuaDateUtils.DAY_MILLIS;

        if (!accountServicePlan.getDataStorageEnabled() || ttl == LocalServicePlan.DISABLED) {
            logger.debug("Storage not enabled for account {}, skipping delete", scopeId);
            return;
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);

        ChannelInfoListResult channels = this.query(scopeId, query);

        // TODO Improve performances
        for (ChannelInfo channelInfo : channels)
            this.mediator.onBeforeChannelInfoDelete(scopeId, channelInfo);

        EsChannelInfoDAO.client(ElasticsearchClient.getInstance())
                  .index(indexName)
                  .deleteByQuery(query);
    }

}
