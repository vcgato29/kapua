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
package org.eclipse.kapua.service.authorization.shiro;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

import org.eclipse.kapua.KapuaException;
import org.eclipse.kapua.commons.model.id.KapuaEid;
import org.eclipse.kapua.commons.security.KapuaSecurityUtils;
import org.eclipse.kapua.locator.KapuaLocator;
import org.eclipse.kapua.model.id.KapuaId;
import org.eclipse.kapua.service.authorization.access.AccessInfo;
import org.eclipse.kapua.service.authorization.access.AccessInfoCreator;
import org.eclipse.kapua.service.authorization.access.AccessInfoFactory;
import org.eclipse.kapua.service.authorization.access.AccessInfoService;
import org.eclipse.kapua.service.authorization.access.AccessPermissionListResult;
import org.eclipse.kapua.service.authorization.access.AccessPermissionService;
import org.eclipse.kapua.service.authorization.access.AccessRoleListResult;
import org.eclipse.kapua.service.authorization.access.AccessRoleService;
import org.eclipse.kapua.service.authorization.domain.Domain;
import org.eclipse.kapua.service.authorization.permission.Actions;
import org.eclipse.kapua.service.authorization.permission.Permission;
import org.eclipse.kapua.service.authorization.permission.PermissionFactory;
import org.eclipse.kapua.service.authorization.role.Role;
import org.eclipse.kapua.service.authorization.role.RoleCreator;
import org.eclipse.kapua.service.authorization.role.RoleFactory;
import org.eclipse.kapua.service.authorization.role.RoleService;
import org.eclipse.kapua.service.user.User;
import org.eclipse.kapua.service.user.UserCreator;
import org.eclipse.kapua.service.user.UserFactory;
import org.eclipse.kapua.service.user.UserService;
import org.eclipse.kapua.test.KapuaTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class AccessInfoServiceTest extends KapuaTest {

    public static String DEFAULT_FILTER = "athz_*.sql";
    public static String DROP_FILTER = "athz_*_drop.sql";

    private static final Domain testDomain = new TestDomain();

    KapuaEid scope = new KapuaEid(BigInteger.valueOf(random.nextLong()));

    // Database fixtures

    @BeforeClass
    public static void beforeClass() throws KapuaException {
        enableH2Connection();
        scriptSession(AuthorizationEntityManagerFactory.getInstance(), DEFAULT_FILTER);
    }

    @AfterClass
    public static void afterClass() throws KapuaException {
        scriptSession(AuthorizationEntityManagerFactory.getInstance(), DROP_FILTER);
    }

    // Tests

    @Test
    public void testSimpleCreate()
            throws Exception {

        KapuaSecurityUtils.doPriviledge(() -> {
            KapuaLocator locator = KapuaLocator.getInstance();

            // Create mock user
            UserService userService = locator.getService(UserService.class);
            UserFactory userFactory = locator.getFactory(UserFactory.class);
            UserCreator userCreator = userFactory.newCreator(scope, "test-user-" + System.currentTimeMillis());
            User user = userService.create(userCreator);

            // Create access info
            AccessInfoService accessInfoService = locator.getService(AccessInfoService.class);
            AccessInfoFactory accessInfoFactory = locator.getFactory(AccessInfoFactory.class);
            AccessInfoCreator accessInfoCreator = accessInfoFactory.newCreator(scope);
            accessInfoCreator.setUserId(user.getId());

            AccessInfo accessInfo = accessInfoService.create(accessInfoCreator);

            assertNotNull(accessInfo);
            assertEquals(accessInfoCreator.getUserId(), accessInfo.getUserId());
            return null;
        });
    }

    @Test
    public void testPermissionCreate()
            throws Exception {

        KapuaSecurityUtils.doPriviledge(() -> {
            KapuaLocator locator = KapuaLocator.getInstance();

            // Create mock user
            UserService userService = locator.getService(UserService.class);
            UserFactory userFactory = locator.getFactory(UserFactory.class);
            UserCreator userCreator = userFactory.newCreator(scope, "test-user-" + System.currentTimeMillis());
            User user = userService.create(userCreator);

            // Create permission
            PermissionFactory permissionFactory = locator.getFactory(PermissionFactory.class);
            Permission permission = permissionFactory.newPermission(testDomain, Actions.read, scope);
            Set<Permission> permissions = new HashSet<>();
            permissions.add(permission);

            // Create access info
            AccessInfoService accessInfoService = locator.getService(AccessInfoService.class);
            AccessInfoFactory accessInfoFactory = locator.getFactory(AccessInfoFactory.class);
            AccessInfoCreator accessInfoCreator = accessInfoFactory.newCreator(scope);
            accessInfoCreator.setUserId(user.getId());
            accessInfoCreator.setPermissions(permissions);

            AccessInfo accessInfo = accessInfoService.create(accessInfoCreator);

            //
            // Verify
            assertNotNull(accessInfo);

            // Access Permissions
            AccessPermissionService accessPermissionService = locator.getService(AccessPermissionService.class);
            AccessPermissionListResult accessPermissions = accessPermissionService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessPermissions);
            assertEquals(accessInfoCreator.getPermissions().size(), accessPermissions.getSize());
            return null;
        });
    }

    @Test
    public void testRoleCreate()
            throws Exception {

        KapuaSecurityUtils.doPriviledge(() -> {
            KapuaLocator locator = KapuaLocator.getInstance();

            // Create mock user
            UserService userService = locator.getService(UserService.class);
            UserFactory userFactory = locator.getFactory(UserFactory.class);
            UserCreator userCreator = userFactory.newCreator(scope, "test-user-" + System.currentTimeMillis());
            User user = userService.create(userCreator);

            // Create permission
            PermissionFactory permissionFactory = locator.getFactory(PermissionFactory.class);
            Permission permission = permissionFactory.newPermission(testDomain, Actions.read, scope);
            Set<Permission> permissions = new HashSet<>();
            permissions.add(permission);

            // Create role
            RoleService roleService = locator.getService(RoleService.class);
            RoleFactory roleFactory = locator.getFactory(RoleFactory.class);
            RoleCreator roleCreator = roleFactory.newCreator(scope);
            roleCreator.setName("testRole-" + System.currentTimeMillis());
            roleCreator.setPermissions(permissions);
            Role role = roleService.create(roleCreator);

            Set<KapuaId> roleIds = new HashSet<>();
            roleIds.add(role.getId());

            // Create access info
            AccessInfoService accessInfoService = locator.getService(AccessInfoService.class);
            AccessInfoFactory accessInfoFactory = locator.getFactory(AccessInfoFactory.class);
            AccessInfoCreator accessInfoCreator = accessInfoFactory.newCreator(scope);
            accessInfoCreator.setUserId(user.getId());
            accessInfoCreator.setRoleIds(roleIds);

            AccessInfo accessInfo = accessInfoService.create(accessInfoCreator);

            //
            // Verify
            assertNotNull(accessInfo);

            // Access Roles
            AccessRoleService accessRoleService = locator.getService(AccessRoleService.class);
            AccessRoleListResult accessRoles = accessRoleService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessRoles);
            assertEquals(accessInfoCreator.getRoleIds().size(), accessRoles.getSize());

            return null;
        });
    }

    @Test
    public void testFind()
            throws Exception {

        KapuaSecurityUtils.doPriviledge(() -> {
            KapuaLocator locator = KapuaLocator.getInstance();

            // Create mock user
            UserService userService = locator.getService(UserService.class);
            UserFactory userFactory = locator.getFactory(UserFactory.class);
            UserCreator userCreator = userFactory.newCreator(scope, "test-user-" + System.currentTimeMillis());
            User user = userService.create(userCreator);

            // Create permission
            PermissionFactory permissionFactory = locator.getFactory(PermissionFactory.class);
            Permission permissionRole = permissionFactory.newPermission(testDomain, Actions.read, scope);
            Set<Permission> permissionsRole = new HashSet<>();
            permissionsRole.add(permissionRole);

            Permission permission = permissionFactory.newPermission(testDomain, Actions.read, scope);
            Set<Permission> permissions = new HashSet<>();
            permissions.add(permission);

            // Create role
            RoleService roleService = locator.getService(RoleService.class);
            RoleFactory roleFactory = locator.getFactory(RoleFactory.class);
            RoleCreator roleCreator = roleFactory.newCreator(scope);
            roleCreator.setName("testRole-" + System.currentTimeMillis());
            roleCreator.setPermissions(permissionsRole);
            Role role = roleService.create(roleCreator);

            Set<KapuaId> roleIds = new HashSet<>();
            roleIds.add(role.getId());

            // Create access info
            AccessInfoService accessInfoService = locator.getService(AccessInfoService.class);
            AccessInfoFactory accessInfoFactory = locator.getFactory(AccessInfoFactory.class);
            AccessInfoCreator accessInfoCreator = accessInfoFactory.newCreator(scope);
            accessInfoCreator.setUserId(user.getId());
            accessInfoCreator.setRoleIds(roleIds);
            accessInfoCreator.setPermissions(permissions);

            AccessInfo accessInfo = accessInfoService.create(accessInfoCreator);

            assertNotNull(accessInfo);

            // Access Permissions
            AccessPermissionService accessPermissionService = locator.getService(AccessPermissionService.class);
            AccessPermissionListResult accessPermissions = accessPermissionService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessPermissions);
            assertEquals(accessInfoCreator.getPermissions().size(), accessPermissions.getSize());

            // Access Roles
            AccessRoleService accessRoleService = locator.getService(AccessRoleService.class);
            AccessRoleListResult accessRoles = accessRoleService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessRoles);
            assertEquals(accessInfoCreator.getRoleIds().size(), accessRoles.getSize());

            //
            // Find
            AccessInfo accessInfoFound = accessInfoService.find(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessInfoFound);
            return null;
        });
    }

    @Test
    public void testDelete()
            throws Exception {

        KapuaSecurityUtils.doPriviledge(() -> {
            KapuaLocator locator = KapuaLocator.getInstance();

            // Create mock user
            UserService userService = locator.getService(UserService.class);
            UserFactory userFactory = locator.getFactory(UserFactory.class);
            UserCreator userCreator = userFactory.newCreator(scope, "test-user-" + System.currentTimeMillis());
            User user = userService.create(userCreator);

            // Create permission
            PermissionFactory permissionFactory = locator.getFactory(PermissionFactory.class);
            Permission permission = permissionFactory.newPermission(testDomain, Actions.read, scope);
            Set<Permission> permissions = new HashSet<>();
            permissions.add(permission);

            // Create role
            RoleService roleService = locator.getService(RoleService.class);
            RoleFactory roleFactory = locator.getFactory(RoleFactory.class);
            RoleCreator roleCreator = roleFactory.newCreator(scope);
            roleCreator.setName("testRole-" + System.currentTimeMillis());
            roleCreator.setPermissions(permissions);
            Role role = roleService.create(roleCreator);

            Set<KapuaId> roleIds = new HashSet<>();
            roleIds.add(role.getId());

            // Create access info
            AccessInfoService accessInfoService = locator.getService(AccessInfoService.class);
            AccessInfoFactory accessInfoFactory = locator.getFactory(AccessInfoFactory.class);
            AccessInfoCreator accessInfoCreator = accessInfoFactory.newCreator(scope);
            accessInfoCreator.setUserId(user.getId());
            accessInfoCreator.setRoleIds(roleIds);

            AccessInfo accessInfo = accessInfoService.create(accessInfoCreator);

            assertNotNull(accessInfo);

            // Access Permissions
            AccessPermissionService accessPermissionService = locator.getService(AccessPermissionService.class);
            AccessPermissionListResult accessPermissions = accessPermissionService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessPermissions);
            assertEquals(accessInfoCreator.getPermissions().size(), accessPermissions.getSize());

            // Access Roles
            AccessRoleService accessRoleService = locator.getService(AccessRoleService.class);
            AccessRoleListResult accessRoles = accessRoleService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessRoles);
            assertEquals(accessInfoCreator.getRoleIds().size(), accessRoles.getSize());
            Role roleFound = roleService.find(role.getScopeId(), role.getId());
            assertNotNull(roleFound);

            //
            // Test delete
            accessInfoService.delete(accessInfo.getScopeId(), accessInfo.getId());

            //
            // Verify
            AccessInfo accessInfoFound = accessInfoService.find(accessInfo.getScopeId(), accessInfo.getId());
            assertNull(accessInfoFound);

            // Access Permissions
            accessPermissions = accessPermissionService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessPermissions);
            assertEquals(0, accessPermissions.getSize());

            // Access Roles
            accessRoles = accessRoleService.findByAccessInfoId(accessInfo.getScopeId(), accessInfo.getId());

            assertNotNull(accessRoles);
            assertEquals(0, accessRoles.getSize());

            // Verify role not deleted
            roleFound = roleService.find(role.getScopeId(), role.getId());
            assertNotNull(roleFound);

            return null;
        });
    }
}
