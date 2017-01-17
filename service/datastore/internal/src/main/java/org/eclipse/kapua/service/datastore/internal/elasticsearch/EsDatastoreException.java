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

public class EsDatastoreException extends Exception 
{
    private static final long serialVersionUID = -2766345175377211253L;

    public EsDatastoreException(String message) {
		super(message);
	}
    
    public EsDatastoreException(Throwable t) {
    	super(t);
    }
    
    public EsDatastoreException(String reason, Throwable t) {
    	super(reason, t);
    }
}
