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

import java.util.Map;

import org.eclipse.kapua.KapuaIllegalArgumentException;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.message.KapuaMessage;
import org.eclipse.kapua.message.KapuaPayload;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.ClientInfoRegistryService;
import org.eclipse.kapua.service.datastore.MetricInfoRegistryService;
import org.eclipse.kapua.service.datastore.ChannelInfoRegistryService;
import org.eclipse.kapua.service.datastore.internal.ClientInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.MessageStoreFacade;
import org.eclipse.kapua.service.datastore.internal.MetricInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.ChannelInfoRegistryFacade;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema.Metadata;
import org.eclipse.kapua.service.datastore.internal.model.ClientInfoCreatorImpl;
import org.eclipse.kapua.service.datastore.internal.model.MetricInfoCreatorImpl;
import org.eclipse.kapua.service.datastore.internal.model.ChannelInfoCreatorImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MessageQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.MetricInfoQueryImpl;
import org.eclipse.kapua.service.datastore.internal.model.query.ChannelMatchPredicateImpl;
import org.eclipse.kapua.service.datastore.model.ClientInfo;
import org.eclipse.kapua.service.datastore.model.MetricInfo;
import org.eclipse.kapua.service.datastore.model.ChannelInfo;

public class DatastoreMediator implements MessageStoreMediator,
										ClientInfoRegistryMediator,
										ChannelInfoRegistryMediator,
										MetricInfoRegistryMediator
{

	private static DatastoreMediator instance ;
	
	private final EsSchema esSchema;
	
	private MessageStoreFacade messageStoreFacade;
	private ClientInfoRegistryFacade clientInfoStoreFacade;
	private ChannelInfoRegistryFacade channelInfoStoreFacade;
	private MetricInfoRegistryFacade metricInfoStoreFacade;
	
	static {
		instance = new DatastoreMediator();
		
		// Be sure the data registry services are instantiated
		KapuaLocator.getInstance().getService(ClientInfoRegistryService.class);
		KapuaLocator.getInstance().getService(ChannelInfoRegistryService.class);
		KapuaLocator.getInstance().getService(MetricInfoRegistryService.class);
	}
	
	private DatastoreMediator()
	{
		this.esSchema = new EsSchema();
	}
	
	public static DatastoreMediator getInstance()
	{
		return instance;
	}

	public void setMessageStoreFacade(MessageStoreFacade messageStoreFacade)
	{
		this.messageStoreFacade = messageStoreFacade;
	}

	public void setClientInfoStoreFacade(ClientInfoRegistryFacade clientInfoStoreFacade)
	{
		this.clientInfoStoreFacade = clientInfoStoreFacade;
	}

	public void setChannelInfoStoreFacade(ChannelInfoRegistryFacade channelInfoStoreFacade)
	{
		this.channelInfoStoreFacade = channelInfoStoreFacade;
	}

	public void setMetricInfoStoreFacade(MetricInfoRegistryFacade metricInfoStoreFacade)
	{
		this.metricInfoStoreFacade = metricInfoStoreFacade;
	}
	
	/*
	 * Message Store Mediator methods
	 */
	
	@Override
	public Metadata getMetadata(KapuaId scopeId, long indexedOn) 
			throws EsDocumentBuilderException, EsClientUnavailableException 
	{
		return this.esSchema.synch(scopeId, indexedOn);
	}

	@Override
	public void onUpdatedMappings(KapuaId scopeId, long indexedOn, Map<String, EsMetric> esMetrics) 
			throws EsDocumentBuilderException, EsClientUnavailableException 
	{
		this.esSchema.updateMessageMappings(scopeId, indexedOn, esMetrics);
	}

	@Override
	public void onAfterMessageStore(KapuaId scopeId,
									MessageXContentBuilder docBuilder, 
									KapuaMessage<?,?> message) 
			throws KapuaIllegalArgumentException,
				   EsDocumentBuilderException, 
				   EsClientUnavailableException, 
				   EsConfigurationException 
	{
		ClientInfoCreatorImpl clientInfoCreator = new ClientInfoCreatorImpl(docBuilder.getAccountName());
		clientInfoCreator.setClientId(docBuilder.getClientId());
		clientInfoCreator.setLastMessageId(docBuilder.getMessageId());
		clientInfoCreator.setLastMessageTimestamp(docBuilder.getTimestamp());
		this.clientInfoStoreFacade.store(scopeId, clientInfoCreator);
		
		ChannelInfoCreatorImpl channelInfoCreator = new ChannelInfoCreatorImpl(docBuilder.getAccountName());
		channelInfoCreator.setClientId(docBuilder.getClientId());
		channelInfoCreator.setChannel(docBuilder.getChannel());
		channelInfoCreator.setLastMessageId(docBuilder.getMessageId());
		channelInfoCreator.setLastMessageTimestamp(docBuilder.getTimestamp());
		this.channelInfoStoreFacade.store(scopeId, channelInfoCreator);
		
		KapuaPayload payload = message.getPayload();
		if  (payload == null)
			return;
		
		Map<String, Object> metrics = payload.getProperties();
		if (metrics == null)
			return;
		
		int i = 0;
		MetricInfoCreatorImpl[] messageMetricsCreators = new MetricInfoCreatorImpl[metrics.size()];
		for(Map.Entry<String, Object> entry:metrics.entrySet()) {
			MetricInfoCreatorImpl metricInfoCreator = new MetricInfoCreatorImpl(docBuilder.getAccountName());
			metricInfoCreator.setDevice(docBuilder.getClientId());
			metricInfoCreator.setChannel(docBuilder.getChannel());
			metricInfoCreator.setName(entry.getKey());
			metricInfoCreator.setType(EsUtils.getEsTypeFromValue(entry.getValue()));
			metricInfoCreator.setLastMessageId(docBuilder.getMessageId());
			metricInfoCreator.setLastMessageTimestamp(docBuilder.getTimestamp());
			metricInfoCreator.setValue(entry.getValue());
			messageMetricsCreators[i++] = metricInfoCreator;
		}
		
		this.metricInfoStoreFacade.store(scopeId, messageMetricsCreators);
	}
	
	/*
	 * ClientInfo Store Mediator methods
	 */

	@Override
	public void onAfterClientInfoDelete(KapuaId scopeId, ClientInfo clientInfo) 
			throws KapuaIllegalArgumentException, 
				   EsConfigurationException, 
				   EsClientUnavailableException 
	{
		this.messageStoreFacade.delete(scopeId, clientInfo.getLastMessageId());
	}
	
	/*
	 * ChannelInfo Store Mediator methods
	 */

	@Override
	public void onBeforeChannelInfoDelete(KapuaId scopeId, ChannelInfo channelInfo) 
			throws KapuaIllegalArgumentException, 
				   EsConfigurationException, 
				   EsQueryConversionException, 
				   EsClientUnavailableException 
	{
        MessageQueryImpl mqi = new MessageQueryImpl();
        ChannelMatchPredicateImpl predicate = new ChannelMatchPredicateImpl(channelInfo.getChannel());
        mqi.setPredicate(predicate);
        this.messageStoreFacade.delete(scopeId, mqi);

        MetricInfoQueryImpl miqi = new MetricInfoQueryImpl();
        mqi.setPredicate(predicate);
        this.metricInfoStoreFacade.delete(scopeId, miqi);
	}

	@Override
	public void onAfterChannelInfoDelete(KapuaId scopeId, ChannelInfo channelInfo) 
	{
		// TODO Auto-generated method stub
		
	}
	
	/*
	 * MetricInfo Store Mediator methods
	 */

	@Override
	public void onAfterMetricInfoDelete(KapuaId scopeId, MetricInfo metricInfo) 
	{
		// TODO Auto-generated method stub
		
	}
}
