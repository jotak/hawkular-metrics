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
package org.hawkular.metrics.api.jaxrs.handler.observer;

import java.net.URI;

import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.exception.StoreKeyAlreadyExistsException;

/**
 * An implementation of {@code EntityCreatedObserver} for metric entities.
 *
 * @author Joel Takvorian
 */
public class StoreEntryCreatedObserver extends EntityCreatedObserver<StoreKeyAlreadyExistsException> {

    public StoreEntryCreatedObserver(AsyncResponse asyncResponse, URI location) {
        super(
                asyncResponse,
                location,
                StoreKeyAlreadyExistsException.class,
                StoreEntryCreatedObserver::getAlreadyExistsResponse
        );
    }

    private static Response getAlreadyExistsResponse(StoreKeyAlreadyExistsException e) {
        String message = "A store entry with key [" + e.getEntry().getKey() + "] already exists";
        return Response.status(Status.CONFLICT).entity(new ApiError(message)).build();
    }
}
