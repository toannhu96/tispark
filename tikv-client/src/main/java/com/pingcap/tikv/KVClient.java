/*
 *
 * Copyright 2019 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.pingcap.tikv;

import static com.pingcap.tikv.util.ClientUtils.getBatches;
import static com.pingcap.tikv.util.ClientUtils.getKvPairs;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.exception.GrpcException;
import com.pingcap.tikv.exception.TiKVException;
import com.pingcap.tikv.operation.iterator.ConcreteScanIterator;
import com.pingcap.tikv.region.RegionStoreClient;
import com.pingcap.tikv.region.RegionStoreClient.RegionStoreClientBuilder;
import com.pingcap.tikv.region.TiRegion;
import com.pingcap.tikv.util.BackOffFunction;
import com.pingcap.tikv.util.BackOffer;
import com.pingcap.tikv.util.Batch;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tikv.kvproto.Kvrpcpb.KvPair;

public class KVClient implements AutoCloseable {
  private static final Logger logger = LoggerFactory.getLogger(KVClient.class);
  private static final int MAX_BATCH_LIMIT = 1024;
  private static final int BATCH_GET_SIZE = 16 * 1024;
  private final RegionStoreClientBuilder clientBuilder;
  private final TiConfiguration conf;
  private final ExecutorService batchGetThreadPool;

  public KVClient(
      TiConfiguration conf,
      RegionStoreClientBuilder clientBuilder,
      ExecutorService batchGetThreadPool) {
    Objects.requireNonNull(conf, "conf is null");
    Objects.requireNonNull(clientBuilder, "clientBuilder is null");
    this.conf = conf;
    this.clientBuilder = clientBuilder;
    this.batchGetThreadPool = batchGetThreadPool;
  }

  @Override
  public void close() {}

  /**
   * Get a key-value pair from TiKV if key exists
   *
   * @param key key
   * @return a ByteString value if key exists, ByteString.EMPTY if key does not exist
   */
  public ByteString get(ByteString key, long version) throws GrpcException {
    BackOffer backOffer = ConcreteBackOffer.newGetBackOff();
    while (true) {
      RegionStoreClient client = clientBuilder.build(key);
      try {
        return client.get(backOffer, key, version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
      }
    }
  }

  /**
   * Get a set of key-value pair by keys from TiKV
   *
   * @param backOffer
   * @param keys
   * @param version
   * @return
   * @throws GrpcException
   */
  public List<KvPair> batchGet(BackOffer backOffer, List<ByteString> keys, long version)
      throws GrpcException {
    return doSendBatchGet(backOffer, keys, version);
  }

  /**
   * Scan key-value pairs from TiKV in range [startKey, endKey)
   *
   * @param startKey start key, inclusive
   * @param endKey end key, exclusive
   * @return list of key-value pairs in range
   */
  public List<KvPair> scan(ByteString startKey, ByteString endKey, long version)
      throws GrpcException {
    Iterator<KvPair> iterator = scanIterator(conf, clientBuilder, startKey, endKey, version);
    List<KvPair> result = new ArrayList<>();
    iterator.forEachRemaining(result::add);
    return result;
  }

  private List<KvPair> doSendBatchGet(BackOffer backOffer, List<ByteString> keys, long version) {
    ExecutorCompletionService<List<KvPair>> completionService =
        new ExecutorCompletionService<>(batchGetThreadPool);

    List<Batch> batches =
        getBatches(backOffer, keys, BATCH_GET_SIZE, MAX_BATCH_LIMIT, this.clientBuilder);

    for (Batch batch : batches) {
      completionService.submit(
          () -> doSendBatchGetInBatchesWithRetry(batch.getBackOffer(), batch, version));
    }

    return getKvPairs(completionService, batches, BackOffer.BATCH_GET_MAX_BACKOFF);
  }

  private List<KvPair> doSendBatchGetInBatchesWithRetry(
      BackOffer backOffer, Batch batch, long version) {
    TiRegion oldRegion = batch.getRegion();
    TiRegion currentRegion =
        clientBuilder.getRegionManager().getRegionByKey(oldRegion.getStartKey());

    if (oldRegion.equals(currentRegion)) {
      RegionStoreClient client = clientBuilder.build(batch.getRegion());
      try {
        return client.batchGet(backOffer, batch.getKeys(), version);
      } catch (final TiKVException e) {
        backOffer.doBackOff(BackOffFunction.BackOffFuncType.BoRegionMiss, e);
        clientBuilder.getRegionManager().invalidateRegion(batch.getRegion());
        logger.warn("ReSplitting ranges for BatchGetRequest", e);

        // retry
        return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
      }
    } else {
      return doSendBatchGetWithRefetchRegion(backOffer, batch, version);
    }
  }

  private List<KvPair> doSendBatchGetWithRefetchRegion(
      BackOffer backOffer, Batch batch, long version) {
    List<Batch> retryBatches =
        getBatches(backOffer, batch.getKeys(), BATCH_GET_SIZE, MAX_BATCH_LIMIT, this.clientBuilder);

    ArrayList<KvPair> results = new ArrayList<>();
    for (Batch retryBatch : retryBatches) {
      // recursive calls
      List<KvPair> batchResult =
          doSendBatchGetInBatchesWithRetry(retryBatch.getBackOffer(), retryBatch, version);
      results.addAll(batchResult);
    }
    return results;
  }

  private Iterator<KvPair> scanIterator(
      TiConfiguration conf,
      RegionStoreClientBuilder builder,
      ByteString startKey,
      ByteString endKey,
      long version) {
    return new ConcreteScanIterator(conf, builder, startKey, endKey, version);
  }
}
