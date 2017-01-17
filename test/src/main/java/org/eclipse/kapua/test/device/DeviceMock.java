/*******************************************************************************
 * Copyright (c) 2011, 2016 Eurotech and/or its affiliates
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Eurotech - initial API and implementation
 *******************************************************************************/
package org.eclipse.kapua.test.device;

import java.math.BigInteger;
import java.util.Date;
import java.util.Properties;

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.model.id.KapuaEid;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.device.registry.Device;
import org.eclipse.kapua.service.device.registry.DeviceCredentialsMode;
import org.eclipse.kapua.service.device.registry.DeviceEventType;
import org.eclipse.kapua.service.device.registry.DeviceStatus;

public class DeviceMock implements Device {

	private static long longId = 1;
    private KapuaEid id;
    private KapuaId scopeId;
    private String clientId;
    	
	public DeviceMock(KapuaId scopeId, String clientId) {
        this.id = new KapuaEid(BigInteger.valueOf(longId++));
        this.scopeId = scopeId;
        this.clientId = clientId;
	}
	@Override
	public Date getModifiedOn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KapuaId getModifiedBy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getOptlock() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setOptlock(int optlock) {
		// TODO Auto-generated method stub

	}

	@Override
	public Properties getEntityAttributes() throws KapuaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setEntityAttributes(Properties props) throws KapuaException {
		// TODO Auto-generated method stub

	}

	@Override
	public Properties getEntityProperties() throws KapuaException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setEntityProperties(Properties props) throws KapuaException {
		// TODO Auto-generated method stub

	}

	@Override
	public KapuaId getId() {
		return this.id;
	}

	@Override
	public void setId(KapuaId id) {
		this.id = (KapuaEid) id;
	}

	@Override
	public KapuaId getScopeId() {
		return this.scopeId;
	}

	@Override
	public Date getCreatedOn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public KapuaId getCreatedBy() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getClientId() {
		// TODO Auto-generated method stub
		return this.clientId;
	}

	@Override
	public void setClientId(String clientId) {
		// TODO Auto-generated method stub
		this.clientId = clientId;
	}

	@Override
	public DeviceStatus getStatus() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStatus(DeviceStatus status) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getDisplayName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setDisplayName(String diplayName) {
		// TODO Auto-generated method stub

	}

	@Override
	public Date getLastEventOn() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLastEventOn(Date lastEventOn) {
		// TODO Auto-generated method stub

	}

	@Override
	public DeviceEventType getLastEventType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setLastEventType(DeviceEventType lastEventType) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getSerialNumber() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setSerialNumber(String serialNumber) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getModelId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setModelId(String modelId) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getImei() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setImei(String imei) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getImsi() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setImsi(String imsi) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getIccid() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setIccid(String iccid) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getBiosVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBiosVersion(String biosVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getFirmwareVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setFirmwareVersion(String firmwareVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getOsVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOsVersion(String osVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getJvmVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setJvmVersion(String jvmVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getOsgiFrameworkVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setOsgiFrameworkVersion(String osgiFrameworkVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getApplicationFrameworkVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setApplicationFrameworkVersion(String appFrameworkVersion) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getApplicationIdentifiers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setApplicationIdentifiers(String applicationIdentifiers) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getAcceptEncoding() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAcceptEncoding(String acceptEncoding) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCustomAttribute1() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCustomAttribute1(String customAttribute1) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCustomAttribute2() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCustomAttribute2(String customAttribute2) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCustomAttribute3() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCustomAttribute3(String customAttribute3) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCustomAttribute4() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCustomAttribute4(String customAttribute4) {
		// TODO Auto-generated method stub

	}

	@Override
	public String getCustomAttribute5() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCustomAttribute5(String customAttribute5) {
		// TODO Auto-generated method stub

	}

	@Override
	public DeviceCredentialsMode getCredentialsMode() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setCredentialsMode(DeviceCredentialsMode credentialsMode) {
		// TODO Auto-generated method stub

	}

	@Override
	public KapuaId getPreferredUserId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setPreferredUserId(KapuaId deviceUserId) {
		// TODO Auto-generated method stub

	}
	@Override
	public KapuaId getConnectionId() {
		// TODO Auto-generated method stub
		return null;
	}
	@Override
	public void setConnectionId(KapuaId connectionId) {
		// TODO Auto-generated method stub
		
	}

}
