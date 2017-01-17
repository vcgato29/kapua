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
import org.eclipse.kapua.service.datastore.internal.elasticsearch.ElasticsearchClient;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsClientUnavailableException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsConfigurationException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsDocumentBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsObjectBuilderException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsQueryConversionException;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema.Metadata;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.LocalServicePlan;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MetricInfoRegistryMediator;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.MetricInfoXContentBuilder;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.dao.EsMetricInfoDAO;
import org.eclipse.kapua.service.datastore.internal.model.MetricInfoListResultImpl;
import org.eclipse.kapua.service.datastore.internal.model.StorableIdImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.AndPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.IdsPredicateImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MetricInfoQueryImpl;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfoCreator;
import org.eclipse.kapua.service.datastore.model.MetricInfoListResult;
import org.eclipse.kapua.service.datastore.model.StorableId;
import org.eclipse.kapua.service.datastore.model.query.MetricInfoQuery;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.index.engine.DocumentAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricInfoRegistryFacade 
{
    // @SuppressWarnings("unused")
    private static final Logger  logger           = LoggerFactory.getLogger(MetricInfoRegistryFacade.class);

    private final MetricInfoRegistryMediator mediator;
	private final ConfigurationProvider configProvider;
	private final Object metadataUpdateSync;

	public MetricInfoRegistryFacade(ConfigurationProvider configProvider, MetricInfoRegistryMediator mediator)
	{
		this.configProvider = configProvider;
		this.mediator = mediator;
		this.metadataUpdateSync = new Object();
	}

	public StorableId store(KapuaId scopeId, MetricInfoCreator metricInfoCreator) 
			throws KapuaIllegalArgumentException, 
				   EsDocumentBuilderException, 
				   EsClientUnavailableException, 
				   EsConfigurationException
	{
		//
		// Argument Validation
		ArgumentValidator.notNull(scopeId, "scopeId");
		ArgumentValidator.notNull(metricInfoCreator, "metricInfoCreator");
		ArgumentValidator.notNull(metricInfoCreator.getLastMessageTimestamp(), "metricInfoCreator.lastMessageTimestamp");
        
		String metricInfoId = MetricInfoXContentBuilder.getOrCreateId(null, metricInfoCreator);

		// Store channel. Look up channel in the cache, and cache it if it doesn't exist
		if (!DatastoreCacheManager.getInstance().getMetricsCache().get(metricInfoId)) {

			// The code is safe even without the synchronized block
			// Synchronize in order to let the first thread complete its
			// update then the others of the same type will find the cache 
			// updated and skip the update.
			synchronized (this.metadataUpdateSync) 
			{
				if (!DatastoreCacheManager.getInstance().getChannelsCache().get(metricInfoId)) {
					UpdateResponse response = null;
					try 
					{
						Metadata metadata = this.mediator.getMetadata(scopeId, metricInfoCreator.getLastMessageTimestamp().getTime());
						String kapuaIndexName = metadata.getKapuaIndexName();

						response = EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
											 .index(metadata.getKapuaIndexName())
											 .upsert(metricInfoCreator);
						
						metricInfoId = response.getId();
						
						logger.debug(String.format("Upsert on metric succesfully executed [%s.%s, %s]",
									 kapuaIndexName, EsSchema.METRIC_TYPE_NAME, metricInfoId));
						
					} catch (DocumentAlreadyExistsException exc) {
						logger.trace(String.format("Upsert failed because metric already exists [%s, %s]",
									metricInfoId, exc.getMessage()));
					}
					// Update cache if channel update is completed successfully
					DatastoreCacheManager.getInstance().getChannelsCache().put(metricInfoId, true);
				}
			}
		}
		return new StorableIdImpl(metricInfoId);
	}

	public StorableId[] store(KapuaId scopeId, MetricInfoCreator[] metricInfoCreators) 
			throws KapuaIllegalArgumentException, 
				   EsDocumentBuilderException, 
				   EsClientUnavailableException, 
				   EsConfigurationException
	{
		//
		// Argument Validation
		ArgumentValidator.notNull(scopeId, "scopeId");
		ArgumentValidator.notNull(metricInfoCreators, "metricInfoCreator");

        // Create a bulk request
		BulkRequest bulkRequest = new BulkRequest();
		for (MetricInfoCreator metricInfoCreator:metricInfoCreators)
		{
			String metricInfoId = MetricInfoXContentBuilder.getOrCreateId(null, metricInfoCreator);
					
			if (DatastoreCacheManager.getInstance().getMetricsCache().get(metricInfoId))
				continue;

			Metadata metadata = this.mediator.getMetadata(scopeId, metricInfoCreator.getLastMessageTimestamp().getTime());
			String kapuaIndexName = metadata.getKapuaIndexName();

			EsMetricInfoDAO.client(ElasticsearchClient.getInstance()).index(kapuaIndexName);
			
			bulkRequest.add(EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
					   .index(kapuaIndexName)
					   .getUpsertRequest(metricInfoCreator));
		}
		
		StorableId[] idResults = null;
		
		// The code is safe even without the synchronized block
		// Synchronize in order to let the first thread complete its update
		// then the others of the same type will find the cache updated and
		// skip the update.
		synchronized (this.metadataUpdateSync) 
		{
			BulkResponse response = EsMetricInfoDAO.client(ElasticsearchClient.getInstance()).bulk(bulkRequest);
			BulkItemResponse[] itemResponses = response.getItems();
			idResults = new StorableId[itemResponses.length];
			
			if (itemResponses != null) {
				for (BulkItemResponse bulkItemResponse : itemResponses) {
					if (bulkItemResponse.isFailed()) {
						MetricInfoCreator failedMetricInfoCreator = metricInfoCreators[bulkItemResponse.getItemId()];
						String failureMessage = bulkItemResponse.getFailureMessage();
						logger.trace(String.format("Upsert failed [%s, %s, %s]",
									 failedMetricInfoCreator.getChannel(), failedMetricInfoCreator.getName(), failureMessage));
						continue;
					}
					
					String channelMetricId = ((UpdateResponse) bulkItemResponse.getResponse()).getId();
					idResults[bulkItemResponse.getItemId()] = new StorableIdImpl(channelMetricId);

					String kapuaIndexName = bulkItemResponse.getIndex();
					String channelTypeName = bulkItemResponse.getType();
					logger.debug(String.format("Upsert on channel metric succesfully executed [%s.%s, %s]",
								 kapuaIndexName, channelTypeName, channelMetricId));

					if (DatastoreCacheManager.getInstance().getMetricsCache().get(channelMetricId))
						continue;

					// Update cache if channel metric update is completed
					// successfully
					DatastoreCacheManager.getInstance().getMetricsCache().put(channelMetricId, true);
				}
			}
		}
		return idResults;
	}

    public void delete(KapuaId scopeId, StorableId id) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsClientUnavailableException
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
        EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
                   .index(indexName)
                   .deleteById(id.toString());
    }

    public MetricInfo find(KapuaId scopeId, StorableId id) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsQueryConversionException, 
    			   EsClientUnavailableException, 
    			   EsObjectBuilderException
    {
        //
        // Argument Validation
        ArgumentValidator.notNull(scopeId, "scopeId");
        ArgumentValidator.notNull(id, "id");

        MetricInfoQueryImpl q = new MetricInfoQueryImpl();
        q.setLimit(1);

        ArrayList<StorableId> ids = new ArrayList<StorableId>();
        ids.add(id);

        AndPredicateImpl allPredicates = new AndPredicateImpl();
        allPredicates.addPredicate(new IdsPredicateImpl(EsSchema.MESSAGE_TYPE_NAME, ids));

        MetricInfoListResult result = this.query(scopeId, q);
        if (result == null || result.size() == 0)
            return null;

        MetricInfo metricInfo = result.get(0);
        return metricInfo;
    }

    public MetricInfoListResult query(KapuaId scopeId, MetricInfoQuery query) 
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
            return new MetricInfoListResultImpl();
        }

        String indexNme = EsSchema.getKapuaIndexName(scopeId);
        MetricInfoListResult result = null;
        result = EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
                            .index(indexNme)
                            .query(query);

        return result;
    }

    public long count(KapuaId scopeId, MetricInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsClientUnavailableException, 
    			   EsQueryConversionException
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
        result = EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
                            .index(indexName)
                            .count(query);

        return result;
    }

    public void delete(KapuaId scopeId, MetricInfoQuery query) 
    		throws KapuaIllegalArgumentException, 
    			   EsConfigurationException, 
    			   EsClientUnavailableException, 
    			   EsQueryConversionException
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
        }

        String indexName = EsSchema.getKapuaIndexName(scopeId);
        EsMetricInfoDAO.client(ElasticsearchClient.getInstance())
                   .index(indexName)
                   .deleteByQuery(query);

        return;
    }
}
