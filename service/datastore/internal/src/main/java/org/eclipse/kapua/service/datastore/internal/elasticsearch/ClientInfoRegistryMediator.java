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

import org.eclipse.kapua.KapuaIllegalArgumentException;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.datastore.internal.elasticsearch.EsSchema.Metadata;
import org.eclipse.kapua.service.datastore.model.ClientInfo;

public interface ClientInfoRegistryMediator 
{
	public Metadata getMetadata(KapuaId scopeId, long indexedOn) 
			throws EsDocumentBuilderException, EsClientUnavailableException;

	public void onAfterClientInfoDelete(KapuaId scopeId, ClientInfo clientInfo)
			throws KapuaIllegalArgumentException, 
			   EsConfigurationException, 
			   EsClientUnavailableException;
}
