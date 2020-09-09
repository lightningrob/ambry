package com.github.ambry.account;

import com.github.ambry.account.mysql.AccountDao;
import com.github.ambry.account.mysql.ContainerDao;
import com.github.ambry.account.mysql.MySqlConfig;
import com.github.ambry.account.mysql.MySqlDataAccessor;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Wrapper class to handle MySql store operations on Account and Container tables
 */
public class MySqlAccountStore {

  private final MySqlDataAccessor mySqlDataAccessor;
  private final AccountDao accountDao;
  private final ContainerDao containerDao;

  public MySqlAccountStore(MySqlConfig config) throws SQLException {
    mySqlDataAccessor = new MySqlDataAccessor(config);
    accountDao = new AccountDao(mySqlDataAccessor);
    containerDao = new ContainerDao(mySqlDataAccessor);
  }

  /**
   * Adds new {@link Account}s to Account table in MySql DB
   * @param accounts collection of {@link Account}s to be inserted
   * @throws SQLException
   */
  public void addAccounts(Collection<Account> accounts) throws SQLException {
    for (Account account : accounts) {
      accountDao.addAccount(account);
    }
  }

  /**
   * Adds new {@link Container}s to Container table in MySql DB
   * @param containers collection of {@link Container}s to be inserted
   * @throws SQLException
   */
  public void addContainers(Collection<Container> containers) throws SQLException {
    for (Container container : containers) {
      containerDao.addContainer(container.getParentAccountId(), container);
    }
  }

  /**
   * Updates existing {@link Account}s in Account table in MySql DB
   * @param accounts collection of {@link Account}s to be updated
   * @throws SQLException
   */
  public void updateAccounts(Collection<Account> accounts) throws SQLException {
    for (Account account : accounts) {
      accountDao.updateAccount(account);
    }
  }

  /**
   * Updates existing {@link Container}s in Container table in MySql DB
   * @param containers collection of {@link Account}s to be updated
   * @throws SQLException
   */
  public void updateContainers(Collection<Container> containers) throws SQLException {
    for (Container container : containers) {
      containerDao.updateContainer(container.getParentAccountId(), container);
    }
  }

  /**
   * Gets all {@link Account}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Account}s
   * @throws SQLException
   */
  public List<Account> getNewAccounts(long updatedSince) throws SQLException {
    return accountDao.getNewAccounts(updatedSince);
  }

  /**
   * Gets all {@link Container}s that have been created or modified since the specified time.
   * @param updatedSince the last modified time used to filter.
   * @return a list of {@link Container}s
   * @throws SQLException
   */
  public List<Container> getNewContainers(long updatedSince) throws SQLException {
    return containerDao.getNewContainers(updatedSince);
  }

  /**
   * Gets all {@link Container}s of a given account
   * @param accountId ID of the account
   * @return a list of {@link Container}s
   * @throws SQLException
   */
  public List<Container> getContainersByAccount(short accountId) throws SQLException {
    return containerDao.getContainers(accountId);
  }

}
