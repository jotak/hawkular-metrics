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
package org.hawkular.metrics.model;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonCreator.Mode;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize.Inclusion;
import com.google.common.base.MoreObjects;

import io.swagger.annotations.ApiModelProperty;

/**
 * @author jtakvori
 */
public class StoreEntry {
    private final String key;
    private final String value;
    private final Map<String, String> tags;

    @JsonCreator(mode = Mode.PROPERTIES)
    public StoreEntry(
            @JsonProperty("key")
            String key,
            @JsonProperty("value")
            String value,
            @JsonProperty(value = "tags")
            Map<String, String> tags
    ) {
        checkArgument(key != null, "Entry key is null");
        this.key = key;
        this.value = value;
        this.tags = tags == null ? emptyMap() : unmodifiableMap(tags);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @ApiModelProperty("Entry tags")
    @JsonSerialize(include = Inclusion.NON_EMPTY)
    public Map<String, String> getTags() {
        return tags;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreEntry entry = (StoreEntry) o;
        return Objects.equals(key, entry.key) &&
                Objects.equals(value, entry.value) &&
                Objects.equals(tags, entry.tags);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, tags);
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("key", key)
                .add("value", value)
                .add("tags", tags)
                .toString();
    }
}
