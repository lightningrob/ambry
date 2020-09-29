/*
 * Copyright 2019 LinkedIn Corp. All rights reserved.
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

package com.github.ambry.frontend;

import com.codahale.metrics.MetricRegistry;
import com.github.ambry.account.Account;
import com.github.ambry.account.AccountCollectionSerde;
import com.github.ambry.account.Container;
import com.github.ambry.account.ContainerBuilder;
import com.github.ambry.account.InMemAccountService;
import com.github.ambry.config.FrontendConfig;
import com.github.ambry.config.VerifiableProperties;
import com.github.ambry.rest.MockRestRequest;
import com.github.ambry.rest.MockRestResponseChannel;
import com.github.ambry.rest.RestMethod;
import com.github.ambry.rest.RestRequest;
import com.github.ambry.rest.RestResponseChannel;
import com.github.ambry.rest.RestServiceErrorCode;
import com.github.ambry.rest.RestServiceException;
import com.github.ambry.rest.RestTestUtils;
import com.github.ambry.rest.RestUtils;
import com.github.ambry.router.FutureResult;
import com.github.ambry.router.ReadableStreamChannel;
import com.github.ambry.utils.TestUtils;
import com.github.ambry.utils.ThrowingBiConsumer;
import com.github.ambry.utils.ThrowingConsumer;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.json.JSONObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class PostAccountContainersHandlerTest {
  private final FrontendTestSecurityServiceFactory securityServiceFactory;
  private final InMemAccountService accountService;
  private final PostAccountsHandler handler;
  private final Account theAccount;

  public PostAccountContainersHandlerTest() {
    FrontendMetrics metrics = new FrontendMetrics(new MetricRegistry());
    securityServiceFactory = new FrontendTestSecurityServiceFactory();
    accountService = new InMemAccountService(false, true);
    handler = new PostAccountsHandler(securityServiceFactory.getSecurityService(), accountService,
        new FrontendConfig(new VerifiableProperties(new Properties())), metrics);
    theAccount = accountService.createAndAddRandomAccount();
  }

  /**
   * Test valid request cases.
   * @throws Exception
   */
  @Test
  public void validRequestsTest() throws Exception {
    String accountName = theAccount.getName();
    short accountId = theAccount.getId();
    ThrowingConsumer<Collection<Container>> testAction = inputContainers -> {
      String requestBody = AccountCollectionSerde.containersToJson(inputContainers).toString();
      RestResponseChannel restResponseChannel = new MockRestResponseChannel();
      RestRequest request = createRestRequest(requestBody, accountName, null);
      ReadableStreamChannel responseChannel = sendRequestGetResponse(request, restResponseChannel);
      assertNotNull("Date has not been set", restResponseChannel.getHeader(RestUtils.Headers.DATE));
      assertEquals("Content-length is not as expected", responseChannel.getSize(),
          Integer.parseInt((String) restResponseChannel.getHeader(RestUtils.Headers.CONTENT_LENGTH)));
      Collection<Container> outputContainers =
          AccountCollectionSerde.containersFromJson(RestTestUtils.getJsonizedResponseBody(responseChannel), accountId);
      assertEquals("Unexpected count returned", inputContainers.size(), outputContainers.size());
      for (Container container : outputContainers) {
        assertEquals("Container in account service not as expected", container,
            accountService.getContainer(accountName, container.getName()));
      }
    };

    // add new container
    testAction.accept(Collections.singleton(accountService.getRandomContainer(accountId)));

    // add multiple containers
    List<Container> containerList = new ArrayList<>();
    for (int j = 0; j < 10; j++) {
      containerList.add(
          new ContainerBuilder(Container.UNKNOWN_CONTAINER_ID, "Test-" + j, Container.ContainerStatus.ACTIVE, "",
              accountId).build());
    }
    testAction.accept(containerList);

    // TODO: update existing containers when support is added
  }

  /**
   * Test bad request cases.
   * @throws Exception
   */
  @Test
  public void badRequestsTest() throws Exception {
    ThrowingBiConsumer<RestRequest, RestServiceErrorCode> testAction = (request, expectedErrorCode) -> {
      TestUtils.assertException(RestServiceException.class,
          () -> sendRequestGetResponse(request, new MockRestResponseChannel()),
          e -> assertEquals("Unexpected error code", expectedErrorCode, e.getErrorCode()));
    };
    String accountName = theAccount.getName();
    // Empty container list should fail
    String emptyContainers = AccountCollectionSerde.containersToJson(Collections.emptyList()).toString();
    RestRequest request = createRestRequest(emptyContainers, accountName, null);
    testAction.accept(request, RestServiceErrorCode.BadRequest);

    // non json input
    request = createRestRequest("ABC", accountName, null);
    testAction.accept(request, RestServiceErrorCode.BadRequest);
    // invalid json
    String invalidJson = new JSONObject().append("accounts", "ABC").toString();
    request = createRestRequest(invalidJson, accountName, null);
    testAction.accept(request, RestServiceErrorCode.BadRequest);
    // No account specified
    String oneContainer = AccountCollectionSerde.containersToJson(
        Collections.singleton(accountService.getRandomContainer(theAccount.getId()))).toString();
    request = createRestRequest(oneContainer, null, null);
    testAction.accept(request, RestServiceErrorCode.BadRequest);
    // AccountService update failure
    accountService.setShouldUpdateSucceed(false);
    request = createRestRequest(oneContainer, accountName, null);
    testAction.accept(request, RestServiceErrorCode.BadRequest);
  }

  /**
   * Creates a {@link RestRequest} for a /accounts/updateContainers request
   * @param requestBody body of the request in string form.
   * @param accountName if set, add this account name as a request header.
   * @param accountId if set, add this account ID as a request header.
   * @return the {@link RestRequest}
   * @throws Exception
   */
  private RestRequest createRestRequest(String requestBody, String accountName, String accountId) throws Exception {
    JSONObject data = new JSONObject();
    data.put(MockRestRequest.REST_METHOD_KEY, RestMethod.POST.name());
    data.put(MockRestRequest.URI_KEY, Operations.UPDATE_ACCOUNT_CONTAINERS);
    JSONObject headers = new JSONObject();
    if (accountName != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_NAME, accountName);
    }
    if (accountId != null) {
      headers.put(RestUtils.Headers.TARGET_ACCOUNT_ID, accountId);
    }
    data.put(MockRestRequest.HEADERS_KEY, headers);
    List<ByteBuffer> body = new LinkedList<>();
    body.add(ByteBuffer.wrap(requestBody.getBytes(StandardCharsets.UTF_8)));
    body.add(null);
    return new MockRestRequest(data, body);
  }

  /**
   * Sends the given {@link RestRequest} to the {@link GetAccountsHandler} and waits for the response and returns it.
   * @param restRequest the {@link RestRequest} to send.
   * @param restResponseChannel the {@link RestResponseChannel} where headers will be set.
   * @return the response body as a {@link ReadableStreamChannel}.
   * @throws Exception
   */
  private ReadableStreamChannel sendRequestGetResponse(RestRequest restRequest, RestResponseChannel restResponseChannel)
      throws Exception {
    FutureResult<ReadableStreamChannel> future = new FutureResult<>();
    handler.handle(restRequest, restResponseChannel, future::done);
    try {
      return future.get(1, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      throw e.getCause() instanceof Exception ? (Exception) e.getCause() : new Exception(e.getCause());
    }
  }
}
