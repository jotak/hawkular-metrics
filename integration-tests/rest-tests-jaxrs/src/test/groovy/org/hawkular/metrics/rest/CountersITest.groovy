/*
 * Copyright 2014-2016 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.hawkular.metrics.rest

import static java.lang.Double.NaN
import static org.joda.time.DateTime.now
import static org.junit.Assert.assertArrayEquals
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue

import org.joda.time.DateTime
import org.junit.Test
import org.hawkular.metrics.core.service.DateTimeService

/**
 * @author John Sanda
 */
class CountersITest extends RESTTest {
  def tenantId = nextTenantId()

  @Test
  void shouldNotAcceptInvalidTimeRange() {
    badGet(path: "counters/test/data", headers: [(tenantHeaderName): tenantId],
        query: [start: 1000, end: 500]) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAcceptInvalidBucketConfig() {
    badGet(path: "counters/test/data", headers: [(tenantHeaderName): tenantId],
        query: [start: 500, end: 100, buckets: '10', bucketDuration: '10ms']) { exception ->
      assertEquals("Should fail when both bucket params are specified", 400, exception.response.status)
    }
  }

  @Test
  void shouldNotCreateMetricWithEmptyPayload() {
    badPost(path: "counters", headers: [(tenantHeaderName): tenantId], body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataForCounterWithEmptyPayload() {
    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/pimpo/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void shouldNotAddDataWithEmptyPayload() {
    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: "" /* Empty Body */) { exception ->
      assertEquals(400, exception.response.status)
    }

    badPost(path: "counters/data", headers: [(tenantHeaderName): tenantId],
        body: [] /* Empty List */) { exception ->
      assertEquals(400, exception.response.status)
    }
  }

  @Test
  void createSimpleCounter() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [tenantId: tenantId, id: id, type: 'counter', dataRetention: 7]
    assertEquals(expectedData, response.data)
  }

  @Test
  void shouldNotCreateDuplicateCounter() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: id]
    )
    assertEquals(201, response.status)

    badPost(path: 'counters', headers: [(tenantHeaderName): tenantId], body: [id: id]) { exception ->
      assertEquals(409, exception.response.status)
    }
  }

  @Test
  void createCounterWithTagsAndDataRetention() {
    String id = "C1"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: id,
            tags: [tag1: 'one', tag2: 'two'],
            dataRetention: 100
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$id", headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    def expectedData = [
        tenantId: tenantId,
        id: id,
        tags: [tag1: 'one', tag2: 'two'],
        dataRetention: 100,
        type: 'counter'
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void createAndFindCounters() {
    String counter1 = "C1"
    String counter2 = "C2"

    def response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [id: counter1]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters",
        headers: [(tenantHeaderName): tenantId],
        body: [
            id: counter2,
            tags: [tag1: 'one', tag2: 'two']
        ]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.get(
        path: "metrics",
        headers: [(tenantHeaderName): tenantId],
        query: [type: 'counter']
    )
    assertEquals(200, response.status)

    def expectedData = [
        [
            tenantId: tenantId,
            id: counter1,
            type: 'counter',
            dataRetention: 7
        ],
        [
            tenantId: tenantId,
            id: counter2,
            tags: [
                tag1: 'one',
                tag2: 'two'
            ],
            type: 'counter',
            dataRetention: 7
        ]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForMultipleCountersAndFindWithDateRange() {
    String counter1 = "C1"
    String counter2 = "C2"

    DateTime start = now().minusMinutes(5)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 10],
                    [timestamp: start.plusMinutes(1).millis, value: 20]
                ]
            ],
            [
                id: counter2,
                data: [
                    [timestamp: start.millis, value: 150],
                    [timestamp: start.plusMinutes(1).millis, value: 225],
                    [timestamp: start.plusMinutes(2).millis, value: 300]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(1).millis]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.millis, value: 10]
    ]
    assertEquals(expectedData, response.data)

    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.millis, end: start.plusMinutes(2).millis]
    )
    assertEquals(200, response.status)

    expectedData = [
        [timestamp: start.millis, value: 150],
        [timestamp: start.plusMinutes(1).millis, value: 225]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void addDataForSingleCounterAndFindWithDefaultDateRange() {
    String counter = "C1"
    DateTime start = now().minusHours(8)

    def response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: start.millis, value: 100],
            [timestamp: start.plusHours(1).millis, value :200],
            [timestamp: start.plusHours(4).millis, value: 500],
            [timestamp: now().plusSeconds(30).millis, value: 750]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: start.plusHours(1).millis, value: 200],
        [timestamp: start.plusHours(4).millis, value: 500]
    ]
    assertEquals(expectedData, response.data)
  }

  @Test
  void findWhenThereIsNoData() {
    String counter1 = "C1"
    String counter2 = "C2"
    DateTime start = now().minusHours(3)

    def response = hawkularMetrics.post(
        path: "counters/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [
                id: counter1,
                data: [
                    [timestamp: start.millis, value: 100],
                    [timestamp: start.plusHours(1).millis, value: 150]
                ]
            ]
        ]
    )
    assertEquals(200, response.status)

    // First query a counter that has data but outside of the date range for which data
    // points are available
    response = hawkularMetrics.get(
        path: "counters/$counter1/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: start.minusHours(5).millis, end: start.minusHours(4).millis]
    )
    assertEquals(204, response.status)

    // Now query a counter that has no dat at all
    response = hawkularMetrics.get(
        path: "counters/$counter2/data",
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(204, response.status)
  }

  @Test
  void findCounterStats() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)

    def expectedData = []
    (1..7).each { i ->
      def bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val;
      switch (i) {
        case 1:
          bucketPoint.putAll([min: 0D, avg: 100D, median: 0D, max: 200D, empty: false,
          samples: 2] as Map)
          break;
        case 2:
        case 4:
        case 6:
          val = NaN
          bucketPoint.putAll([min: val, avg: val, median: val, max: val, empty: true,
          samples: 0] as Map)
          break;
        case 3:
          bucketPoint.putAll([min: 400D, avg: 400D, median: 400D, max: 400D, empty: false,
          samples: 1] as Map)
          break;
        case 5:
          bucketPoint.putAll([min: 550D, avg: 550D, median: 550D, max: 550D, empty: false,
          samples: 1] as Map)
          break;
        case 7:
          bucketPoint.putAll([min: 950D, avg: 975D, median: 950D, max: 1000D, empty: false,
          samples: 2] as Map)
          break;
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  @Test
  void findRate() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 0]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: (60_000 * 1.5).toLong(), value: 400],
        [timestamp: (60_000 * 3.5).toLong(), value: 100],
        [timestamp: (60_000 * 5.0).toLong(), value: 100],
        [timestamp: (60_000 * 7.0).toLong(), value: 200],
        [timestamp: (60_000 * 7.5).toLong(), value: 100],
    ]

    def actualData = response.data ?: []

    def msg = """
Expected: ${expectedData}
Actual:   ${response.data}
"""
    assertEquals(msg, expectedData.size(), actualData.size())
    for (i in 0..actualData.size() - 1) {
      assertRateEquals(msg, expectedData[i], actualData[i])
    }
  }

  @Test
  void findRateWhenThereAreResets() {
    String counter = 'C1'

    // Create the tenant
    def response = hawkularMetrics.post(
        path: 'tenants',
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 1],
            [timestamp: 60_000 * 1.5, value: 2],
            [timestamp: 60_000 * 3.5, value: 3],
            [timestamp: 60_000 * 5.0, value: 1],
            [timestamp: 60_000 * 7.0, value: 2],
            [timestamp: 60_000 * 7.5, value: 3],
            [timestamp: 60_000 * 8.0, value: 1],
            [timestamp: 60_000 * 8.5, value: 2],
            [timestamp: 60_000 * 9.0, value: 3]
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 0]
    )
    assertEquals(200, response.status)

    def expectedData = [
        [timestamp: (60_000 * 1.5) as Long, value: 2],
        [timestamp: (60_000 * 3.5) as Long, value: 0.5],
        [timestamp: (60_000 * 7.0) as Long, value: 0.5],
        [timestamp: (60_000 * 7.5) as Long, value: 2],
        [timestamp: (60_000 * 8.5) as Long, value: 2],
        [timestamp: (60_000 * 9.0) as Long, value: 2]
    ]

    def actualData = response.data ?: []

    def msg = """
Expected: ${expectedData}
Actual:   ${response.data}
"""
    assertEquals(msg, expectedData.size(), actualData.size())
    for (i in 0..actualData.size() - 1) {
      assertRateEquals(msg, expectedData[i], actualData[i])
    }
  }

  @Test
  void findRateStats() {
    String counter = "C1"

    // Create the tenant
    def response = hawkularMetrics.post(
        path: "tenants",
        body: [id: tenantId]
    )
    assertEquals(201, response.status)

    response = hawkularMetrics.post(
        path: "counters/$counter/data",
        headers: [(tenantHeaderName): tenantId],
        body: [
            [timestamp: 60_000 * 1.0, value: 0],
            [timestamp: 60_000 * 1.5, value: 200],
            [timestamp: 60_000 * 3.5, value: 400],
            [timestamp: 60_000 * 5.0, value: 550],
            [timestamp: 60_000 * 7.0, value: 950],
            [timestamp: 60_000 * 7.5, value: 1000],
        ]
    )
    assertEquals(200, response.status)

    response = hawkularMetrics.get(
        path: "counters/$counter/rate",
        headers: [(tenantHeaderName): tenantId],
        query: [start: 60_000, end: 60_000 * 8, bucketDuration: '1mn']
    )
    assertEquals(200, response.status)

    def expectedData = []
    (1..7).each { i ->
      Map bucketPoint = [start: 60_000 * i, end: 60_000 * (i + 1)]
      double val
      switch (i) {
        case 1:
          val = 400D
          bucketPoint << [min: val, avg: val, median: val, max: val, empty: false, samples: 1]
          break
        case 3:
          val = 100D
          bucketPoint << [min: val, avg: val, median: val, max: val, empty: false, samples: 1]
          break
        case 5:
          val = 100D
          bucketPoint << [min: val, avg: val, median: val, max: val, empty: false, samples: 1]
          break
        case 7:
          bucketPoint << [min: 100.0, max: 200.0, avg: 150.0, median: 100.0, empty:false,
                          samples: 2]
          break
        default:
          bucketPoint << [min: NaN, max: NaN, avg: NaN, median: NaN, empty: true, samples: 0]
          break
      }
      expectedData.push(bucketPoint);
    }

    assertNumericBucketsEquals(expectedData, response.data ?: [])
  }

  static void assertRateEquals(String msg, def expected, def actual) {
    assertEquals(msg, expected.timestamp, actual.timestamp)
    assertDoubleEquals(msg, expected.value, actual.value)
  }

  @Test
  void shouldStoreLargePayload() {
    checkLargePayload("counters", tenantId, { points, i -> points.push([timestamp: i, value: i]) })
  }

  @Test
  void shouldNotAcceptDataWithEmptyTimestamp() {
    invalidPointCheck("counters", tenantId, [[value: "up"]])
  }

  @Test
  void shouldNotAcceptDataWithNullTimestamp() {
    invalidPointCheck("counters", tenantId, [[timestamp: null, value: 1]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidTimestamp() {
    invalidPointCheck("counters", tenantId, [[timestamp: "aaa", value: 1]])
  }

  @Test
  void shouldNotAcceptDataWithEmptyValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13]])
  }

  @Test
  void shouldNotAcceptDataWithNullValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13, value: null]])
  }

  @Test
  void shouldNotAcceptDataWithInvalidValue() {
    invalidPointCheck("counters", tenantId, [[timestamp: 13, value: ["dsqdqs"]]])
  }

  @Test
  void percentileParameter() {
      String counter = "C1"

      // Create the tenant
      def response = hawkularMetrics.post(
          path: "tenants",
          body: [id: tenantId]
      )
      assertEquals(201, response.status)

      response = hawkularMetrics.post(
          path: "counters/$counter/data",
          headers: [(tenantHeaderName): tenantId],
          body: [
              [timestamp: 60_000 * 1.0, value: 0],
              [timestamp: 60_000 * 1.5, value: 200],
              [timestamp: 60_000 * 3.5, value: 400],
              [timestamp: 60_000 * 5.0, value: 550],
              [timestamp: 60_000 * 7.0, value: 950],
              [timestamp: 60_000 * 7.5, value: 1000],
          ]
      )
      assertEquals(200, response.status)

      response = hawkularMetrics.get(
          path: "counters/$counter/data",
          headers: [(tenantHeaderName): tenantId],
          query: [start: 60_000, end: 60_000 * 8, buckets: '1', percentiles: '50.0,90.0,99.9']
      )
      assertEquals(200, response.status)

      assertEquals(1, response.data.size)
      assertEquals(3, response.data[0].percentiles.size)

      assertEquals(0.5, response.data[0].percentiles[0].quantile, 0.01)
      assertEquals(400, response.data[0].percentiles[0].value, 0.1)
  }

  @Test
  void findStackedStatsForMultipleCounters() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/data", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/data',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterBucketByTag.end)
    assertDoubleEquals("The min is wrong", (c1.min {it.value}).value + (c2.min {it.value}).value, actualCounterBucketByTag.min)
    assertDoubleEquals("The max is wrong", (c1.max {it.value}).value + (c2.max {it.value}).value, actualCounterBucketByTag.max)
    assertDoubleEquals("The avg is wrong", avg(c1.collect {it.value}) + avg(c2.collect {it.value}), actualCounterBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/data',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C1', 'C2'],
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    def actualCounterBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("Stacked stats when queried by tag are different than when queried by id", actualCounterBucketById, actualCounterBucketByTag)
  }

  @Test
  void findSimpleStatsForMultipleCounters() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/data", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/data',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2'
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def expectedSimpleCounterBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, expectedSimpleCounterBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, expectedSimpleCounterBucketByTag.end)
    assertDoubleEquals("The min is wrong", (combinedData.min {it.value}).value, expectedSimpleCounterBucketByTag.min)
    assertDoubleEquals("The max is wrong", (combinedData.max {it.value}).value, expectedSimpleCounterBucketByTag.max)
    assertDoubleEquals("The avg is wrong", avg(combinedData.collect {it.value}), expectedSimpleCounterBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, expectedSimpleCounterBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", expectedSimpleCounterBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/data',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1']
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    def actualSimpleCounterBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualSimpleCounterBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualSimpleCounterBucketById.end)
    assertDoubleEquals("The min is wrong", (combinedData.min {it.value}).value, actualSimpleCounterBucketById.min)
    assertDoubleEquals("The max is wrong", (combinedData.max {it.value}).value, actualSimpleCounterBucketById.max)
    assertDoubleEquals("The avg is wrong", avg(combinedData.collect {it.value}), actualSimpleCounterBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualSimpleCounterBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualSimpleCounterBucketById.median != null)
  }

  @Test
  void findStackedStatsForMultipleCounterRates() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/data", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketByTag.end)
    assertDoubleEquals("The min is wrong", (c1Rates.min + c2Rates.min), actualCounterRateBucketByTag.min)
    assertDoubleEquals("The max is wrong", (c1Rates.max + c2Rates.max), actualCounterRateBucketByTag.max)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg), actualCounterRateBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1'],
            stacked: true
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketById.end)
    assertDoubleEquals("The min is wrong", (c1Rates.min + c2Rates.min), actualCounterRateBucketById.min)
    assertDoubleEquals("The max is wrong", (c1Rates.max + c2Rates.max), actualCounterRateBucketById.max)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg), actualCounterRateBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketById.median != null)
  }

  @Test
  void findSimpleStatsForMultipleCounterRates() {
    String tenantId = nextTenantId()
    DateTime start = now().minusMinutes(10)

    // Create some metrics
    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C1',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C2',
        tags: ['type': 'counter_cpu_usage', 'host': 'server2', 'env': 'dev']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: 'counters', body: [
        id  : 'C3',
        tags: ['type': 'counter_cpu_usage', 'host': 'server3', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    Random rand = new Random()
    def randomList = []
    (1..10).each {
      randomList << rand.nextInt(100)
    }
    randomList.sort()

    def c1 = [
      [timestamp: start.millis, value: 510 + randomList[0]],
      [timestamp: start.plusMinutes(1).millis, value: 512 + randomList[1]],
      [timestamp: start.plusMinutes(2).millis, value: 514 + randomList[2]],
      [timestamp: start.plusMinutes(3).millis, value: 516 + randomList[3]],
      [timestamp: start.plusMinutes(4).millis, value: 518 + randomList[4]]
    ]
    def c2 = [
      [timestamp: start.millis, value: 378 + randomList[5]],
      [timestamp: start.plusMinutes(1).millis, value: 381 + randomList[6]],
      [timestamp: start.plusMinutes(2).millis, value: 384 + randomList[7]],
      [timestamp: start.plusMinutes(3).millis, value: 387 + randomList[8]],
      [timestamp: start.plusMinutes(4).millis, value: 390 + randomList[9]]
    ]

    // insert data points
    response = hawkularMetrics.post(path: "counters/data", body: [
        [
            id: 'C1',
            data: c1
        ],
        [
            id: 'C2',
            data: c2
        ],
        [
            id: 'C3',
            data: [
                [timestamp: start.millis, value: 5712],
                [timestamp: start.plusMinutes(1).millis, value: 5773],
                [timestamp: start.plusMinutes(2).millis, value: 5949],
                [timestamp: start.plusMinutes(3).millis, value: 5979],
                [timestamp: start.plusMinutes(4).millis, value: 6548]
            ]
        ]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    c1 = c1.take(c1.size() - 1)
    c2 = c2.take(c2.size() - 1)
    def combinedData = c1 + c2;

    //Get counter rates
    response = hawkularMetrics.get(
        path: 'counters/C1/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c1Rates = response.data[0];

    response = hawkularMetrics.get(
        path: 'counters/C2/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    def c2Rates = response.data[0];

    //Tests start here
    response = hawkularMetrics.get(
        path: 'counters/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            tags: 'type:counter_cpu_usage,host:server1|server2',
            stacked: false
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketByTag = response.data[0]

    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketByTag.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketByTag.end)
    assertDoubleEquals("The min is wrong", Math.min(c1Rates.min, c2Rates.min), actualCounterRateBucketByTag.min)
    assertDoubleEquals("The max is wrong", Math.max(c1Rates.max, c2Rates.max), actualCounterRateBucketByTag.max)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg)/2, actualCounterRateBucketByTag.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketByTag.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketByTag.median != null)

    response = hawkularMetrics.get(
        path: 'counters/rate',
        query: [
            start: start.millis,
            end: start.plusMinutes(4).millis,
            buckets: 1,
            metrics: ['C2', 'C1'],
            stacked: false
        ],
        headers: [(tenantHeaderName): tenantId]
    )
    assertEquals(200, response.status)

    assertEquals("Expected to get back one bucket", 1, response.data.size())

    def actualCounterRateBucketById = response.data[0]

    assertEquals("Expected to get back one bucket", 1, response.data.size())
    assertEquals("The start time is wrong", start.millis, actualCounterRateBucketById.start)
    assertEquals("The end time is wrong", start.plusMinutes(4).millis, actualCounterRateBucketById.end)
    assertDoubleEquals("The min is wrong", Math.min(c1Rates.min, c2Rates.min), actualCounterRateBucketById.min)
    assertDoubleEquals("The max is wrong", Math.max(c1Rates.max, c2Rates.max), actualCounterRateBucketById.max)
    assertDoubleEquals("The avg is wrong", (c1Rates.avg + c2Rates.avg)/2, actualCounterRateBucketById.avg)
    assertEquals("The [empty] property is wrong", false, actualCounterRateBucketById.empty)
    assertTrue("Expected the [median] property to be set", actualCounterRateBucketById.median != null)
  }

  @Test
  void fromEarliestWithData() {
    String tenantId = nextTenantId()
    String metric = "testStats"

    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : '$metric',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.post(path: "counters/$metric/data", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(2).millis, value: 2]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.post(path: "counters/$metric/data", body: [
        [timestamp: new DateTimeService().currentHour().minusHours(3).millis, value: 3]
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    response = hawkularMetrics.get(path: "counters/$metric/data",
        query: [fromEarliest: "true", bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId])

    assertEquals(200, response.status)
    assertEquals(4, response.data.size)

    def expectedArray = [new BigDecimal(3), new BigDecimal(2), 'NaN', 'NaN'].toArray()
    assertArrayEquals(expectedArray, response.data.min.toArray())
    assertArrayEquals(expectedArray, response.data.max.toArray())
    assertArrayEquals(expectedArray, response.data.avg.toArray())
  }

  @Test
  void fromEarliestWithoutDataAndBad() {
    String tenantId = nextTenantId()
    String metric = "testStats"

    def response = hawkularMetrics.post(path: 'counters', body: [
        id  : '$metric',
        tags: ['type': 'counter_cpu_usage', 'host': 'server1', 'env': 'stage']
    ], headers: [(tenantHeaderName): tenantId])
    assertEquals(201, response.status)

    response = hawkularMetrics.get(path: "counters/$metric/data",
      query: [start: 1, end: now().millis, bucketDuration: "1000d"], headers: [(tenantHeaderName): tenantId])
    assertEquals(200, response.status)

    badGet(path: "counters/$metric/data",
        query: [fromEarliest: "true", bucketDuration: "a"], headers: [(tenantHeaderName): tenantId]) {
        exception ->
          assertEquals(400, exception.response.status)
    }

    response = hawkularMetrics.get(path: "counters/$metric/data",
        query: [fromEarliest: "true", bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId])
    assertEquals(204, response.status)
    assertEquals(null, response.data)

    badGet(path: "counters/$metric/data",
      query: [fromEarliest: "true"], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        // From earliest works only with buckets
        assertEquals(400, exception.response.status)
    }

    badGet(path: "counters/$metric/data",
      query: [start: 0, end: Long.MAX_VALUE, fromEarliest: "true", bucketDuration: "1h"], headers: [(tenantHeaderName): tenantId]) {
      exception ->
        // From earliest works only without start & end
        assertEquals(400, exception.response.status)
    }
  }
}
