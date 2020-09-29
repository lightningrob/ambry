/*
 * Copyright 2020 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.github.ambry.account;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.config.MySqlAccountServiceConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.utils.SystemTime;
import com.github.ambry.utils.TestUtils;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.junit.Before;
import org.junit.Test;

import static com.github.ambry.config.MySqlAccountServiceConfig.*;
import static com.github.ambry.utils.TestUtils.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


/**
 * Unit tests for {@link MySqlAccountService}.
 */
public class MySqlAccountServiceTest {

  AccountService mySqlAccountService;
  Properties mySqlConfigProps = new Properties();

  @Before
  public void resetConfig() {
    mySqlConfigProps.setProperty(DB_URL, "");
    mySqlConfigProps.setProperty(DB_USER, "");
    mySqlConfigProps.setProperty(DB_PASSWORD, "");
    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlConfigProps.setProperty(UPDATE_DISABLED, "false");
  }

  /**
   * Tests in-memory cache is initialized with metadata from local file on start up
   * @throws IOException
   */
  @Test
  public void testInitCacheFromDisk() throws IOException, SQLException {
    Path accountBackupDir = Paths.get(TestUtils.getTempDir("account-backup")).toAbsolutePath();
    mySqlConfigProps.setProperty(MySqlAccountServiceConfig.BACKUP_DIRECTORY_KEY, accountBackupDir.toString());

    // write test account to backup file
    long lastModifiedTime = 100;
    Account testAccount =
        new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).lastModifiedTime(lastModifiedTime)
            .build();
    Map<String, String> accountMap = new HashMap<>();
    accountMap.put(Short.toString(testAccount.getId()), testAccount.toJson(false).toString());
    String filename = BackupFileManager.getBackupFilename(1, SystemTime.getInstance().seconds());
    Path filePath = accountBackupDir.resolve(filename);
    BackupFileManager.writeAccountMapToFile(filePath, accountMap);

    MySqlAccountStore mockMySqlAccountStore = mock(MySqlAccountStore.class);
    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStore);

    // verify cache is initialized on startup with test account from backup file
    assertEquals("Mismatch in number of accounts in cache", 1, mySqlAccountService.getAllAccounts().size());
    assertEquals("Mismatch in account info in cache", testAccount,
        mySqlAccountService.getAllAccounts().iterator().next());

    // verify that mySqlAccountStore.getNewAccounts() is called with input argument "lastModifiedTime" value as 100
    verify(mockMySqlAccountStore, atLeastOnce()).getNewAccounts(lastModifiedTime);
    verify(mockMySqlAccountStore, atLeastOnce()).getNewContainers(lastModifiedTime);
  }

  /**
   * Tests in-memory cache is updated with accounts from mysql db store on start up
   */
  @Test
  public void testInitCacheOnStartUp() throws SQLException, IOException {
    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();
    MySqlAccountStore mockMySqlAccountStore = mock(MySqlAccountStore.class);
    when(mockMySqlAccountStore.getNewAccounts(0)).thenReturn(new ArrayList<>(Collections.singletonList(testAccount)));

    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStore);

    // Test in-memory cache is updated with accounts from mysql store on start up.
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account information", testAccount, accounts.get(0));
  }

  /**
   * Tests creating and updating accounts through {@link MySqlAccountService}:
   * 1. add a new {@link Account};
   * 2. update existing {@link Account} by adding new {@link Container} to an existing {@link Account};
   */
  @Test
  public void testUpdateAccounts() throws SQLException, IOException {

    MySqlAccountStore mockMySqlAccountStore = mock(MySqlAccountStore.class);
    when(mockMySqlAccountStore.getNewAccounts(0)).thenReturn(new ArrayList<>());
    Container testContainer =
        new ContainerBuilder((short) 1, "testContainer", Container.ContainerStatus.ACTIVE, "testContainer",
            (short) 1).build();
    Account testAccount = new AccountBuilder((short) 1, "testAccount", Account.AccountStatus.ACTIVE).containers(
        Collections.singleton(testContainer)).build();

    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStore);

    // 1. Addition of new account. Verify account is added to cache.
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    verify(mockMySqlAccountStore, atLeastOnce()).addAccounts(Collections.singletonList(testAccount));
    List<Account> accounts = new ArrayList<>(mySqlAccountService.getAllAccounts());
    assertEquals("Mismatch in number of accounts", 1, accounts.size());
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
    assertEquals("Mismatch in account retrieved by name", testAccount,
        mySqlAccountService.getAccountByName(testAccount.getName()));

    // 2. Update existing account by adding new container. Verify account is updated in cache.
    Container testContainer2 =
        new ContainerBuilder((short) 2, "testContainer2", Container.ContainerStatus.ACTIVE, "testContainer2", (short) 1)
            .build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer2).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(Collections.singletonList(testAccount));
    verify(mockMySqlAccountStore, atLeastOnce()).addContainers(Collections.singletonList(testContainer2));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // 3. Update existing container. Verify container is updated in cache.
    testContainer = new ContainerBuilder(testContainer).setMediaScanDisabled(true).setCacheable(true).build();
    testAccount = new AccountBuilder(testAccount).addOrUpdateContainer(testContainer).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(testAccount));
    verify(mockMySqlAccountStore, atLeastOnce()).updateAccounts(Collections.singletonList(testAccount));
    verify(mockMySqlAccountStore, atLeastOnce()).updateContainers(Collections.singletonList(testContainer));
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));
  }

  /**
   * Tests background updater for updating cache from mysql store periodically.
   */
  @Test
  public void testBackgroundUpdater() throws InterruptedException, SQLException, IOException {

    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "1");
    Account testAccount = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).build();
    MySqlAccountStore mockMySqlAccountStore = mock(MySqlAccountStore.class);
    when(mockMySqlAccountStore.getNewAccounts(anyLong())).thenReturn(new ArrayList<>())
        .thenReturn(new ArrayList<>(Collections.singletonList(testAccount)));

    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStore);

    assertEquals("Background account updater thread should have been started", 1,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));

    // Verify cache is empty.
    assertNull("Cache should be empty", mySqlAccountService.getAccountById(testAccount.getId()));
    // sleep for polling interval time
    Thread.sleep(Long.parseLong(mySqlConfigProps.getProperty(UPDATER_POLLING_INTERVAL_SECONDS)) * 1000 + 100);
    // Verify account is added to cache by background updater.
    assertEquals("Mismatch in account retrieved by ID", testAccount,
        mySqlAccountService.getAccountById(testAccount.getId()));

    // verify that close() stops the background updater thread
    mySqlAccountService.close();

    // force shutdown background updater thread. As the default timeout value is 1 minute, it is possible that thread is
    // present after close() due to actively executing task.
    ((MySqlAccountService) mySqlAccountService).getScheduler().shutdownNow();

    assertEquals("Background account updater thread should be stopped", 0,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));
  }

  /**
   * Tests disabling of background updater by setting {@link MySqlAccountServiceConfig#UPDATER_POLLING_INTERVAL_SECONDS} to 0.
   */
  @Test
  public void testDisableBackgroundUpdater() throws IOException {

    mySqlConfigProps.setProperty(UPDATER_POLLING_INTERVAL_SECONDS, "0");
    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));
    assertEquals("Background account updater thread should not be started", 0,
        numThreadsByThisName(MySqlAccountService.MYSQL_ACCOUNT_UPDATER_PREFIX));
  }

  /**
   * Tests disabling account updates by setting {@link MySqlAccountServiceConfig#UPDATE_DISABLED} to true.
   */
  @Test
  public void testUpdateDisabled() throws IOException {

    mySqlConfigProps.setProperty(UPDATE_DISABLED, "true");
    MySqlAccountStore mockMySqlAccountStore = mock(MySqlAccountStore.class);
    mySqlAccountService = new MySqlAccountService(new AccountServiceMetrics(new MetricRegistry()),
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mockMySqlAccountStore);

    // Verify account update is disabled
    Account testAccount = new AccountBuilder((short) 1, "testAccount1", Account.AccountStatus.ACTIVE).build();
    assertFalse("Update accounts should be disabled",
        mySqlAccountService.updateAccounts(Collections.singletonList(testAccount)));
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in name.
   */
  @Test
  public void testUpdateNameConflictingAccounts() throws IOException {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService = new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 2, "a", Account.AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", mySqlAccountService.updateAccounts(conflictAccounts));
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests updating a collection of {@link Account}s, where the {@link Account}s are conflicting among themselves
   * in id.
   */
  @Test
  public void testUpdateIdConflictingAccounts() throws IOException {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService = new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "b", Account.AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", mySqlAccountService.updateAccounts(conflictAccounts));
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests updating a collection of {@link Account}s, where there are duplicate {@link Account}s in id and name.
   */
  @Test
  public void testUpdateDuplicateAccounts() throws IOException {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService = new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));
    List<Account> conflictAccounts = new ArrayList<>();
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    conflictAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.INACTIVE).build());
    assertFalse("Wrong return value from update operation.", mySqlAccountService.updateAccounts(conflictAccounts));
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());
  }

  /**
   * Tests following cases for name/id conflicts as specified in the JavaDoc of {@link AccountService#updateAccounts(Collection)}.
   *
   * Existing accounts
   * AccountId     AccountName
   *    1             "a"
   *    2             "b"
   *
   * Accounts will be updated in following order
   * Steps   AccountId   AccountName   If Conflict    Treatment                    Conflict reason
   *  A      1           "a"           no             replace existing record      N/A
   *  B      1           "c"           no             replace existing record      N/A
   *  C      3           "d"           no             add a new record             N/A
   *  D      4           "c"           yes            fail update                  conflicts with existing name.
   *  E      1           "b"           yes            fail update                  conflicts with existing name.
   *
   *
   */
  @Test
  public void testConflictingUpdatesWithAccounts() throws IOException {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService = new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));

    // write two accounts (1, "a") and (2, "b")
    List<Account> existingAccounts = new ArrayList<>();
    existingAccounts.add(new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).build());
    existingAccounts.add(new AccountBuilder((short) 2, "b", Account.AccountStatus.ACTIVE).build());
    mySqlAccountService.updateAccounts(existingAccounts);

    // case A: verify updating status of account (1, "a") replaces existing record (1, "a")
    Account accountToUpdate =
        new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).status(Account.AccountStatus.INACTIVE)
            .build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 1));

    // case B: verify updating name of account (1, "a") to (1, "c") replaces existing record (1, "a")
    accountToUpdate = new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).name("c").build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 1));

    // case C: verify adding new account (3, "d") adds new record (3, "d")
    accountToUpdate = new AccountBuilder((short) 3, "d", Account.AccountStatus.ACTIVE).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in account information", accountToUpdate, mySqlAccountService.getAccountById((short) 3));

    // case D: verify adding new account (4, "c") conflicts in name with (1, "c")
    accountToUpdate = new AccountBuilder((short) 4, "c", Account.AccountStatus.ACTIVE).build();
    assertFalse("Account update should fail due to name conflict",
        mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate)));
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case E: verify updating name of account  (1, "c") to (1, "b") conflicts in name with (2, "b")
    accountToUpdate = new AccountBuilder(mySqlAccountService.getAccountById((short) 1)).name("b").build();
    assertFalse("Account update should fail due to name conflict",
        mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate)));
    assertEquals("UpdateAccountErrorCount in metrics should be 2", 2,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // verify there should be 3 accounts (1, "c), (2, "b) and (3, "d) at the end of operation
    assertEquals("Mismatch in number of accounts", 3, mySqlAccountService.getAllAccounts().size());
    assertEquals("Mismatch in name of account", "c", mySqlAccountService.getAccountById((short) 1).getName());
    assertEquals("Mismatch in name of account", "b", mySqlAccountService.getAccountById((short) 2).getName());
    assertEquals("Mismatch in name of account", "d", mySqlAccountService.getAccountById((short) 3).getName());
  }

  /**
   * Tests name/id conflicts in Containers
   */
  @Test
  public void testConflictingUpdatesWithContainers() throws IOException {
    AccountServiceMetrics accountServiceMetrics = new AccountServiceMetrics(new MetricRegistry());
    mySqlAccountService = new MySqlAccountService(accountServiceMetrics,
        new MySqlAccountServiceConfig(new VerifiableProperties(mySqlConfigProps)), mock(MySqlAccountStore.class));

    List<Container> containersList = new ArrayList<>();
    containersList.add(
        new ContainerBuilder((short) 1, "c1", Container.ContainerStatus.ACTIVE, "c1", (short) 1).build());
    containersList.add(
        new ContainerBuilder((short) 2, "c2", Container.ContainerStatus.ACTIVE, "c2", (short) 1).build());
    Account accountToUpdate =
        new AccountBuilder((short) 1, "a", Account.AccountStatus.ACTIVE).containers(containersList).build();

    // write account (1,a) with containers (1,c1), (2,c2)
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in number of containers", 2,
        mySqlAccountService.getAccountById(accountToUpdate.getId()).getAllContainers().size());

    // case A: Verify that changing name of container (1,c1) to (1,c3) replaces existing record
    Container containerToUpdate =
        new ContainerBuilder((short) 1, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getAccountById((short) 1).getContainerById((short) 1));

    // case B: Verify addition of new container (3,c3) conflicts in name with existing container (1,c3)
    containerToUpdate =
        new ContainerBuilder((short) 3, "c3", Container.ContainerStatus.ACTIVE, "c3", (short) 1).build();
    accountToUpdate =
        new AccountBuilder(accountToUpdate).containers(Collections.singletonList(containerToUpdate)).build();
    assertFalse("Account update should fail due to name conflict in containers",
        mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate)));
    accountToUpdate = new AccountBuilder(accountToUpdate).removeContainer(containerToUpdate).build();
    assertEquals("UpdateAccountErrorCount in metrics should be 1", 1,
        accountServiceMetrics.updateAccountErrorCount.getCount());

    // case C: Verify addition of new container (3,c4) is successful
    containerToUpdate =
        new ContainerBuilder((short) 3, "c4", Container.ContainerStatus.ACTIVE, "c4", (short) 1).build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate));
    assertEquals("Mismatch in container information", containerToUpdate,
        mySqlAccountService.getAccountById((short) 1).getContainerById((short) 3));

    // case D: Verify updating name of container (3,c4) to c2 conflicts in name with existing container (2,c2)
    containerToUpdate =
        new ContainerBuilder(mySqlAccountService.getAccountById((short) 1).getContainerById((short) 3)).setName("c2")
            .build();
    accountToUpdate = new AccountBuilder(accountToUpdate).addOrUpdateContainer(containerToUpdate).build();
    assertFalse("Account update should fail due to name conflict in containers",
        mySqlAccountService.updateAccounts(Collections.singletonList(accountToUpdate)));
    assertEquals("UpdateAccountErrorCount in metrics should be 2", 2,
        accountServiceMetrics.updateAccountErrorCount.getCount());
  }
}
