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
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ClientInfoXContentBuilder;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ElasticsearchClient;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsClientUnavailableException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsConfigurationException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsDocumentBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsObjectBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsQueryConversionException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema.Metadata;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.LocalServicePlan;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.dao.EsClientInfoDAO;
import org.eclipse.kapua.service.datastore.internal.model.ClientInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ClientInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.ClientInfoCreator;
import org.eclipse.kapua.service.datastore.model.ClientInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.ClientInfoQuery;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientInfoRegistryFacade 
{
    // @SuppressWarnings("unused")
    private static final Logger logger           = LoggerFactory.getLogger(ClientInfoRegistryFacade.class);

    private final ClientInfoRegistryMediator mediator;
	private final ConfigurationProvider configProvider;
	private final Object metadataUpdateSync;

	public ClientInfoRegistryFacade(ConfigurationProvider configProvider, ClientInfoRegistryMediator mediator)
	{
		this.configProvider = configProvider;
		this.mediator = mediator;
		this.metadataUpdateSync = new Object();
	}

	public StorableId store(KapuaId scopeId, ClientInfoCreator clientInfoCreator) 
			throws KapuaIllegalArgumentException, 
				   EsDocumentBuilderException, 
				   EsClientUnavailableException, 
				   EsConfigurationException 
	{
		//
		// Argument Validation
		ArgumentValidator.notNull(scopeId, "scopeId");
		ArgumentValidator.notNull(clientInfoCreator, "clientInfoCreator");
		ArgumentValidator.notNull(clientInfoCreator.getLastMessageTimestamp(), "clientInfoCreator.lastMessageTimestamp");

        ClientInfoXContentBuilder docBuilder = new ClientInfoXContentBuilder();
		docBuilder.build(clientInfoCreator);
		
		// Save client
		if (!DatastoreCacheManager.getInstance().getClientsCache().get(docBuilder.getClientId())) {

			// The code is safe even without the synchronized block
			// Synchronize in order to let the first thread complete its update
			// then the others of the same type will find the cache updated and
			// skip the update.
			synchronized (this.metadataUpdateSync) {
				if (!DatastoreCacheManager.getInstance().getClientsCache().get(docBuilder.getClientId())) {
					UpdateResponse response = null;
					try 
					{
						Metadata metadata = this.mediator.getMetadata(scopeId, clientInfoCreator.getLastMessageTimestamp().getTime());
						String kapuaIndexName = metadata.getKapuaIndexName();
						
						response = EsClientInfoDAO.client(ElasticsearchClient.getInstance()).index(kapuaIndexName)
								.upsert(docBuilder.getClientId(), docBuilder.getClientBuilder());
						logger.debug(String.format("Upsert on asset succesfully executed [%s.%s, %s]", kapuaIndexName,
								EsSchema.CHANNEL_TYPE_NAME, response.getId()));
					} catch (DocumentAlreadyExistsException exc) {
						logger.trace(String.format("Upsert failed because asset already exists [%s, %s]",
								docBuilder.getClientId(), exc.getMessage()));
					}
					// Update cache if asset update is completed successfully
					DatastoreCacheManager.getInstance().getClientsCache().put(docBuilder.getClientId(), true);
				}
			}
		}
		
		return new StorableIdImpl(docBuilder.getClientId());
	}
	
    public void delete(KapuaId scopeId, StorableId id)
        throws KapuaIllegalArgumentException, 
        	   EsClientUnavailableException, 
        	   EsConfigurationException
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

        String indexName = EsSchema.getDataIndexName(scopeId);
        EsClientInfoDAO.client(ElasticsearchClient.getInstance())
                  .index(indexName)
                  .deleteById(id.toString());
    }

    public ClientInfo find(KapuaId scopeId, StorableId id) 
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

        ClientInfoQueryImpl q = new ClientInfoQueryImpl();
        q.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(EsSchema.CLIENT_TYPE_NAME, ids));

        ClientInfoListResult result = this.query(scopeId, q);
        if (result == null || result.size() == 0)
            return null;

        ClientInfo assetInfo = result.get(0);
        return assetInfo;
    }

    public ClientInfoListResult query(KapuaId scopeId, ClientInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsQueryConversionException, 
    			   EsClientUnavailableException, 
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
            return new ClientInfoListResultImpl();
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);
        ClientInfoListResult result = null;
        result = EsClientInfoDAO.client(ElasticsearchClient.getInstance())
                           .index(indexName)
                           .query(query);

        return result;
    }

    public long count(KapuaId scopeId, ClientInfoQuery query)
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

        String dataIndexName = EsSchema.getKapuaIndexName(scopeId);
        long result;
        result = EsClientInfoDAO.client(ElasticsearchClient.getInstance())
                           .index(dataIndexName)
                           .count(query);

        return result;
    }

    public void delete(KapuaId scopeId, ClientInfoQuery query)
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
            logger.debug("Storage not enabled for account {}, skipping delete", scopeId);
            return;
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);
        EsClientInfoDAO.client(ElasticsearchClient.getInstance())
                  .index(indexName)
                  .deleteByQuery(query);

        return;
    }

}
