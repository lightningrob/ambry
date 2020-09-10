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

import com.github.ambry.account.mysql.MySqlConfig;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.github.ambry.account.AccountUtils.*;


/**
 * An implementation of {@link AccountService} that employs MySql database as its underlying storage.
 */
public class MySqlAccountService implements AccountService {

  private MySqlAccountStore mySqlAccountStore = null;
  private final AccountServiceMetrics accountServiceMetrics;
  private final MySqlConfig mySqlConfig;
  // in-memory cache for storing account and container metadata
  private final AccountInfoMap accountInfoMap;
  private static final Logger logger = LoggerFactory.getLogger(MySqlAccountService.class);
  private final ReadWriteLock rwLock = new ReentrantReadWriteLock();

  public MySqlAccountService(AccountServiceMetrics accountServiceMetrics, MySqlConfig mySqlConfig) {
    this.accountServiceMetrics = accountServiceMetrics;
    this.mySqlConfig = mySqlConfig;
    accountInfoMap = new AccountInfoMap(accountServiceMetrics);
    try {
      createMySqlAccountStore();
    } catch (SQLException e) {
      logger.error("MySQL account store creation failed", e);
      // Continue account service creation. Cache will initialized with metadata from backup copy on local disk to serve read requests.
      // Write requests will be blocked until MySql DB is up. Connection to MySql DB will be retried in polling thread that fetches new accounts.
    }

    // TODO: create backup manager to manage local back up copies of Account and container metadata and lastModifiedTime

    initializeCache();

    // TODO: Start background thread for periodically querying MYSql DB for added/modified accounts and containers. Also, retry creation of MySqlAccountStore if it failed above.

    // TODO: Subscribe to notifications from ZK
  }

  /**
   * creates MySql Account store which establishes connection to database
   * @throws SQLException
   */
  private void createMySqlAccountStore() throws SQLException {
    if (mySqlAccountStore == null) {
      try {
        mySqlAccountStore = new MySqlAccountStore(this.mySqlConfig);
      } catch (SQLException e) {
        logger.error("MySQL account store creation failed", e);
        throw e;
      }
    }
  }

  /**
   * Call to initialize in-memory cache by fetching all the {@link Account}s and {@link Container}s metadata records.
   * It consists of 2 steps:
   * 1. Check local disk for back up copy and load metadata and last modified time of Accounts/Containers into cache.
   * 2. Fetch added/modified accounts and containers from mysql database since the last modified time (found in step 1)
   *    and load into cache.
   */
  void initializeCache() {
    // TODO: Check local disk for back up copy and load metadata and last modified time into cache.
    fetchAndUpdateCache();
  }

  /**
   * Fetches all the accounts and containers that have been created or modified since the last sync time and loads into
   * cache.
   */
  void fetchAndUpdateCache() {
    try {
      // Retry connection to mysql if we couldn't set up previously
      createMySqlAccountStore();
    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
      return;
    }

    // get the last sync time of accounts and containers in cache
    long lastModifiedTime = accountInfoMap.getLastModifiedTime();

    try {
      // Fetch all added/modified accounts from MySql database since LMT
      List<Account> accounts = mySqlAccountStore.getNewAccounts(lastModifiedTime);
      rwLock.writeLock().lock();
      try {
        accountInfoMap.updateAccounts(accounts);
      } finally {
        rwLock.writeLock().unlock();
      }

      // Fetch all added/modified containers from MySql database since LMT
      List<Container> containers = mySqlAccountStore.getNewContainers(lastModifiedTime);
      rwLock.writeLock().lock();
      try {
        accountInfoMap.updateContainers(containers);
      } finally {
        rwLock.writeLock().unlock();
      }

      // TODO: Find the max LMT in the fetched accounts and containers and update the cache

    } catch (SQLException e) {
      logger.error("Fetching Accounts from MySql DB failed", e);
    }
  }

  @Override
  public Account getAccountById(short accountId) {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccountById(accountId);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public Account getAccountByName(String accountName) {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccountByName(accountName);
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public boolean updateAccounts(Collection<Account> accounts) {
    Objects.requireNonNull(accounts, "accounts cannot be null");
    if (accounts.isEmpty()) {
      logger.debug("Empty account collection to update.");
      return false;
    }

    if (mySqlAccountStore == null) {
      logger.info("MySql Account DB store is not accessible");
      return false;
    }

    // TODO: Similar to HelixAccountServiceConfig.updateDisabled, we might need a config to disable account updates when needed

    if (hasDuplicateAccountIdOrName(accounts)) {
      logger.debug("Duplicate account id or name exist in the accounts to update");
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // Make a pre check for conflict between the accounts to update and the accounts in the local cache. Will fail this
    // update operation for all the accounts if any conflict exists. For existing accounts, there is a chance that the account to update
    // conflicts with the accounts in the local cache, but does not conflict with those in the MySql database. This
    // will happen if some accounts are updated but the local cache is not yet refreshed.
    // TODO: Once we have APIs (and versioning) for updates at container granularity, we will need to check conflicts at container level.
    rwLock.readLock().lock();
    try {
      if (accountInfoMap.hasConflictingAccount(accounts)) {
        logger.debug("Accounts={} conflict with the accounts in local cache. Cancel the update operation.", accounts);
        //accountServiceMetrics.updateAccountErrorCount.inc();
        return false;
      }
    } finally {
      rwLock.readLock().unlock();
    }

    try {
      updateAccountsWithMySqlStore(accounts);
    } catch (SQLException e) {
      logger.error("Failed updating accounts={} in MySql DB", accounts, e);
      // record failure, parse exception to figure out what we did wrong (eg. id or name collision). If it is id collision,
      // retry with incrementing ID (Account ID generation logic is currently in nuage-ambry, we might need to move it here)
      //accountServiceMetrics.updateAccountErrorCount.inc();
      return false;
    }

    // update in-memory cache with accounts
    rwLock.writeLock().lock();
    try {
      accountInfoMap.updateAccounts(accounts);
    } finally {
      rwLock.writeLock().unlock();
    }

    // TODO: can notify account updates to other nodes via ZK .

    // TODO: persist updated accounts and max timestamp to local back up file.

    return true;
  }

  @Override
  public Collection<Account> getAllAccounts() {
    rwLock.readLock().lock();
    try {
      return accountInfoMap.getAccounts();
    } finally {
      rwLock.readLock().unlock();
    }
  }

  @Override
  public boolean addAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return false;
  }

  @Override
  public boolean removeAccountUpdateConsumer(Consumer<Collection<Account>> accountUpdateConsumer) {
    return false;
  }

  @Override
  public void close() throws IOException {

  }

  /**
   * Updates MySql DB with added or modified {@link Account}s
   * @param accounts collection of {@link Account}s
   * @throws SQLException
   */
  private void updateAccountsWithMySqlStore(Collection<Account> accounts) throws SQLException {
    long startTimeMs = System.currentTimeMillis();
    logger.trace("Start updating accounts={} into MySql DB", accounts);

    // write Accounts and containers to MySql
    for (Account account : accounts) {
      if (getAccountById(account.getId()) == null) {
        // new account (insert the containers and account into db tables)
        mySqlAccountStore.addAccounts(Collections.singletonList(account));
        mySqlAccountStore.addContainers(account.getAllContainers());
      } else {
        // existing account (update account table)
        mySqlAccountStore.updateAccounts(Collections.singletonList(account));
        updateContainersWithMySqlStore(account.getId(), account.getAllContainers());
      }
    }

    long timeForUpdate = System.currentTimeMillis() - startTimeMs;
    logger.trace("Completed updating accounts into MySql DB, took time={} ms", timeForUpdate);
    //accountServiceMetrics.updateAccountTimeInMs.update(timeForUpdate);

  }

  /**
   * Updates MySql DB with added or modified {@link Container}s of a given account
   * @param accountId id of the {@link Account} for the {@link Container}s
   * @param containers collection of {@link Container}s
   * @throws SQLException
   */
  private void updateContainersWithMySqlStore(short accountId, Collection<Container> containers) throws SQLException {
    //check if account ID should exist first in in-memory cache
    Account accountInCache = accountInfoMap.getAccountById(accountId);
    if (accountInCache == null) {
      throw new IllegalArgumentException("Account with ID " + accountId + "doesn't exist");
    }

    for (Container container : containers) {
      if (accountInfoMap.getContainerByIdForAccount(container.getParentAccountId(), container.getId()) == null) {
        // new container added (insert into container table)
        mySqlAccountStore.addContainers(Collections.singletonList(container));
      } else {
        // existing container modified (update container table)
        mySqlAccountStore.updateContainers(Collections.singletonList(container));
      }
    }
  }
}
