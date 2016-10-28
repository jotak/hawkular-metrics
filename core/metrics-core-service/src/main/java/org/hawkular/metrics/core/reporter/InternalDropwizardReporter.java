/*
 * Copyright 2016 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.reporter;

import static org.hawkular.metrics.model.MetricType.COUNTER;
import static org.hawkular.metrics.model.MetricType.GAUGE;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.model.DataPoint;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.reporter.persister.HawkularMetricsPersister;

import rx.Observable;

/**
 * @author Joel Takvorian
 */
public class InternalDropwizardReporter implements HawkularMetricsPersister {

    private final MetricsService metricsService;
    private final String tenant;

    public InternalDropwizardReporter(MetricsService metricsService, String tenant) {
        this.metricsService = metricsService;
        this.tenant = tenant;
    }

    @Override
    public void writeGaugesData(Long timestamp, Map<String, Double> metricsPoints) throws IOException {
        Observable<Metric<Double>> metrics = Observable.from(metricsPoints.entrySet())
                .map(e -> new Metric<>(
                        new MetricId<Double>(tenant, MetricType.GAUGE, e.getKey()),
                        Collections.singletonList(new DataPoint<>(timestamp, e.getValue()))));
        metricsService.addDataPoints(MetricType.GAUGE, metrics);
    }

    @Override
    public void writeCountersData(Long timestamp, Map<String, Long> metricsPoints) throws IOException {
        Observable<Metric<Long>> metrics = Observable.from(metricsPoints.entrySet())
                .map(e -> new Metric<>(
                        new MetricId<Long>(tenant, MetricType.COUNTER, e.getKey()),
                        Collections.singletonList(new DataPoint<>(timestamp, e.getValue()))));
        metricsService.addDataPoints(MetricType.COUNTER, metrics);
    }

    @Override
    public void writeGaugesTags(String metricId, Map<String, String> tags) throws IOException {
        metricsService.addTags(new Metric<>(new MetricId<>(tenant, GAUGE, metricId)), tags);
    }

    @Override
    public void writeCountersTags(String metricId, Map<String, String> tags) throws IOException {
        metricsService.addTags(new Metric<>(new MetricId<>(tenant, COUNTER, metricId)), tags);
    }

    @Override
    public void addProperties(Map<String, String> map) {
        // Ignoring
    }
}
