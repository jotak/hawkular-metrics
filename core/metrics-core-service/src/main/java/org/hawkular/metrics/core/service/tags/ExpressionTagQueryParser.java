/*
 * Copyright 2014-2017 Red Hat, Inc. and/or its affiliates
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
package org.hawkular.metrics.core.service.tags;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.hawkular.metrics.core.service.DataAccess;
import org.hawkular.metrics.core.service.MetricsService;
import org.hawkular.metrics.core.service.PatternUtil;
import org.hawkular.metrics.core.service.tags.parser.TagQueryBaseListener;
import org.hawkular.metrics.core.service.tags.parser.TagQueryLexer;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ArrayContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ObjectContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.PairContext;
import org.hawkular.metrics.core.service.tags.parser.TagQueryParser.ValueContext;
import org.hawkular.metrics.core.service.transformers.MetricIdFromMetricIndexRowTransformer;
import org.hawkular.metrics.core.service.transformers.StoreKeyFromTagIndexRowTransformer;
import org.hawkular.metrics.core.service.transformers.TagsIndexRowTransformerFilter;
import org.hawkular.metrics.model.Metric;
import org.hawkular.metrics.model.MetricId;
import org.hawkular.metrics.model.MetricType;
import org.hawkular.metrics.model.StoreEntry;

import com.datastax.driver.core.Row;

import rx.Observable;

/**
 * @author Stefan Negrea
 */
public class ExpressionTagQueryParser {

    private DataAccess dataAccess;
    private MetricsService metricsService;

    public ExpressionTagQueryParser(DataAccess dataAccess, MetricsService metricsService) {
        this.dataAccess = dataAccess;
        this.metricsService = metricsService;
    }

    public <T> Observable<Metric<T>> parse(String tenantId, MetricType<T> metricType, String expression) {
        ANTLRInputStream input = new ANTLRInputStream(expression);
        TagQueryLexer tql = new TagQueryLexer(input);
        tql.removeErrorListeners();
        tql.addErrorListener(new ThrowingErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(tql);

        TagQueryParser parser = new TagQueryParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ThrowingErrorListener());

        ParseTree parseTree = parser.tagquery();

        final Function<String, Observable<MetricId<T>>> finderWithoutTag;
        if (metricType == null) {
            finderWithoutTag = tagName -> Observable.from(MetricType.userTypes())
                    .concatMap(type -> {
                        MetricType<T> lType = (MetricType<T>) type;
                        return dataAccess.findMetricsInMetricsIndex(tenantId, type)
                                .filter(r -> r.getMap(1, String.class, String.class).get(tagName) == null)
                                .compose(new MetricIdFromMetricIndexRowTransformer<>(tenantId, lType));
                    }).distinct();
        } else {
            finderWithoutTag = tagName -> dataAccess.findMetricsInMetricsIndex(tenantId, metricType)
                            .filter(r -> r.getMap(1, String.class, String.class).get(tagName) == null)
                            .compose(new MetricIdFromMetricIndexRowTransformer<>(tenantId, metricType))
                            .distinct();
        }

        StorageTagQueryListener<MetricId<T>, Metric<T>> listener = new StorageTagQueryListener<>(
                tagName -> dataAccess.findMetricsByTagName(tenantId, tagName),
                finderWithoutTag,
                metricsService::findMetric,
                () -> new TagsIndexRowTransformerFilter<>(metricType),
                3);
        ParseTreeWalker.DEFAULT.walk(listener, parseTree);

        return listener.getResult();
    }

    public Observable<StoreEntry> parseStoreEntries(String tenantId, String expression) {
        ANTLRInputStream input = new ANTLRInputStream(expression);
        TagQueryLexer tql = new TagQueryLexer(input);
        tql.removeErrorListeners();
        tql.addErrorListener(new ThrowingErrorListener());

        CommonTokenStream tokens = new CommonTokenStream(tql);

        TagQueryParser parser = new TagQueryParser(tokens);
        parser.removeErrorListeners();
        parser.addErrorListener(new ThrowingErrorListener());

        ParseTree parseTree = parser.tagquery();

        final Function<String, Observable<String>> finderWithoutTag =
                tagName -> metricsService.findAllStoreEntries(tenantId)
                    .filter(entry -> entry.getTags().get(tagName) == null)
                    .distinct()
                    .map(StoreEntry::getKey);

        StorageTagQueryListener<String, StoreEntry> listener = new StorageTagQueryListener<>(
                tagName -> dataAccess.findStoreTagsByName(tenantId, tagName),
                finderWithoutTag,
                idx -> metricsService.findStoreEntry(tenantId, idx),
                StoreKeyFromTagIndexRowTransformer::new,
                2);
        ParseTreeWalker.DEFAULT.walk(listener, parseTree);

        return listener.getResult();
    }

    public class ThrowingErrorListener extends BaseErrorListener {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
                String msg, RecognitionException e)
                throws ParseCancellationException {
            throw new ParseCancellationException("line " + line + ":" + charPositionInLine + " " + msg);
        }
    }

    private class StorageTagQueryListener<T,U> extends TagQueryBaseListener {

        private final Map<Integer, List<String>> arrays = new HashMap<>();
        private final Map<Integer, List<Observable<T>>> observables = new HashMap<>();
        private final Function<String, Observable<Row>> finderByTag;
        private final Function<String, Observable<T>> finderWithoutTag;
        private final Function<T, Observable<U>> resolver;
        private final Supplier<Observable.Transformer<Row, T>> transformerSupplier;
        private final int tagValueIndex;

        private StorageTagQueryListener(Function<String, Observable<Row>> finderByTag,
                                        Function<String, Observable<T>> finderWithoutTag,
                                        Function<T, Observable<U>> resolver,
                                        Supplier<Observable.Transformer<Row, T>> transformerSupplier,
                                        int tagValueIndex) {
            this.finderByTag = finderByTag;
            this.finderWithoutTag = finderWithoutTag;
            this.resolver = resolver;
            this.transformerSupplier = transformerSupplier;
            this.tagValueIndex = tagValueIndex;
        }

        public Observable<U> getResult() {
            if (observables.size() == 1) {
                return observables.values().iterator().next().get(0)
                    .flatMap(resolver::apply);
            }
            return Observable.empty();
        }

        @Override
        public void exitPair(PairContext ctx) {
            String tagName = ctx.key().getText();

            Observable<T> result = null;

            if (ctx.array_operator() != null) {
                // extra ' characters are already removed by the array listener
                List<String> valueArray = arrays.get(ctx.array().getText().hashCode());
                List<Pattern> patterns = new ArrayList<>(valueArray.size());
                valueArray.forEach(tagValue -> patterns.add(PatternUtil.filterPattern(tagValue)));
                boolean positive = ctx.array_operator().NOT() == null;

                result = finderByTag.apply(tagName)
                        .filter(r -> {
                            for (Pattern p : patterns) {
                                if (positive && p.matcher(r.getString(tagValueIndex)).matches()) {
                                    return true;
                                } else if (!positive && p.matcher(r.getString(tagValueIndex)).matches()) {
                                    return false;
                                }
                            }

                            return !positive;
                        })
                        .compose(transformerSupplier.get())
                        .distinct();
            } else if (ctx.boolean_operator() != null) {
                String tagValue = null;

                if (ctx.value().COMPLEXTEXT() != null) {
                    tagValue = ctx.value().COMPLEXTEXT().getText();
                    tagValue = tagValue.substring(1, tagValue.length() - 1);
                } else if (ctx.value().SIMPLETEXT() != null) {
                    tagValue = ctx.value().SIMPLETEXT().getText();
                }

                Pattern p = PatternUtil.filterPattern(tagValue);
                boolean positive = ctx.boolean_operator().EQUAL() != null;

                result = finderByTag.apply(tagName)
                        .filter(r -> positive == p.matcher(r.getString(tagValueIndex)).matches())
                        .compose(transformerSupplier.get())
                        .distinct();
            } else if (ctx.existence_operator() != null) {
                if (ctx.existence_operator().NOT() != null) {
                    result = finderWithoutTag.apply(tagName);
                }
            } else {
                result = finderByTag.apply(tagName)
                        .compose(transformerSupplier.get())
                        .distinct();
            }

            pushObservable(ctx.getText().hashCode(), result);
        }

        @Override
        public void exitObject(ObjectContext ctx) {
            if (ctx.logical_operator() != null) {
                Observable<T> leftObservable = popObservable(ctx.object(0).getText().hashCode());
                Observable<T> rightObservable = popObservable(ctx.object(1).getText().hashCode());

                observables.remove(ctx.object(0).getText().hashCode());
                observables.remove(ctx.object(1).getText().hashCode());

                Observable<T> result = leftObservable.concatWith(rightObservable);

                if (ctx.logical_operator().AND() != null) {
                    //group by metric and then use one element from the groups with two elements
                    //if a group has two elements it is in both sets, hence AND
                    result = result
                            .groupBy(m -> m)
                            .flatMap(s -> s.skip(1).take(1));
                } else if (ctx.logical_operator().OR() != null) {
                    result = result.distinct();
                }

                pushObservable(ctx.getText().hashCode(), result);
            } else {
                if (ctx.object(0) != null && ctx.object(0).getText().hashCode() != ctx.getText().hashCode()) {
                    Observable<T> expressionObservable = popObservable(ctx.object(0).getText().hashCode());
                    observables.remove(ctx.object(0).getText().hashCode());
                    pushObservable(ctx.getText().hashCode(), expressionObservable);
                }
            }
        }

        @Override
        public void enterArray(ArrayContext ctx) {
            List<String> arrayContext = new ArrayList<>();
            for (ValueContext node : ctx.value()) {
                if (node.COMPLEXTEXT() != null) {
                    String text = node.COMPLEXTEXT().getText();
                    arrayContext.add(text.substring(1, text.length() - 1));
                } else if (node.SIMPLETEXT() != null) {
                    String text = node.SIMPLETEXT().getText();
                    arrayContext.add(text);
                }

            }
            arrays.put(ctx.getText().hashCode(), arrayContext);
        }

        private void pushObservable(Integer hashCode, Observable<T> observable) {
            List<Observable<T>> hashObservables = observables.get(hashCode);
            if (hashObservables != null) {
                hashObservables.add(observable);
            } else {
                hashObservables = new ArrayList<>();
                hashObservables.add(observable);
                observables.put(hashCode, hashObservables);
            }
        }

        private Observable<T> popObservable(Integer hashCode) {
            List<Observable<T>> hashObservables = observables.get(hashCode);

            if (hashObservables == null || hashObservables.isEmpty()) {
                return null;
            }

            Observable<T> observable = hashObservables.remove(0);

            if (hashObservables.isEmpty()) {
                observables.remove(hashCode);
            }

            return observable;
        }
    }
}
