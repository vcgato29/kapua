/*******************************************************************************
 * Copyright (c) 2011, 2016 Eurotech and/or its affiliates and others
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *
 *******************************************************************************/
package org.eclipse.kapua.app.console.server;

import java.util.UUID;

import org.eclipse.kapua.app.console.setting.ConsoleSetting;
import org.eclipse.kapua.app.console.setting.ConsoleSettingKeys;
import org.eclipse.kapua.app.console.shared.service.GwtSettingsService;

import com.google.gwt.user.server.rpc.RemoteServiceServlet;

/**
 * This is the security token service, a concrete implementation to fix the XSFR security problem.
 */
public class GwtSettingsServiceImpl extends RemoteServiceServlet implements
        GwtSettingsService {

    private static final long serialVersionUID = -6876999298300071273L;
    private static final ConsoleSetting settings = ConsoleSetting.getInstance();
    
    @Override
    public String getLoginBackgroundCredits() {
        return settings.getString(ConsoleSettingKeys.LOGIN_BACKGROUND_CREDITS);
    }

    @Override
    public String getSsoLoginUri() {
        return settings.getString(ConsoleSettingKeys.SSO_OPENID_SERVER_ENDPOINT_AUTH) + "?scope=openid&response_type=code&client_id=" + settings.getString(ConsoleSettingKeys.SSO_OPENID_CLIENT_ID)+ "&state=" + UUID.randomUUID() + "&redirect_uri=" + settings.getString(ConsoleSettingKeys.SSO_OPENID_REDIRECT_URI);
    }

    @Override
    public boolean getSsoEnabled() {
        return settings.getBoolean(ConsoleSettingKeys.SSO_ENABLE);
    }
}
