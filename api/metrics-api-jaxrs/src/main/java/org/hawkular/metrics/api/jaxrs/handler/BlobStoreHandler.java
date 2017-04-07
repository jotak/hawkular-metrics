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
package org.hawkular.metrics.api.jaxrs.handler;

import static javax.ws.rs.core.MediaType.APPLICATION_JSON;

import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.badRequest;
import static org.hawkular.metrics.api.jaxrs.util.ApiUtils.serverError;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.regex.PatternSyntaxException;

import javax.enterprise.context.ApplicationScoped;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.hawkular.metrics.api.jaxrs.BlobStoreQueryRequest;
import org.hawkular.metrics.api.jaxrs.handler.observer.ResultSetObserver;
import org.hawkular.metrics.api.jaxrs.handler.observer.StoreEntryCreatedObserver;
import org.hawkular.metrics.api.jaxrs.util.ApiUtils;
import org.hawkular.metrics.api.jaxrs.util.Logged;
import org.hawkular.metrics.model.ApiError;
import org.hawkular.metrics.model.StoreEntry;
import org.hawkular.metrics.model.exception.RuntimeApiError;
import org.hawkular.metrics.model.param.TagNames;
import org.hawkular.metrics.model.param.Tags;
import org.jboss.resteasy.annotations.GZIP;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import rx.Observable;

/**
 * @author jtakvorian
 */
@Path("/blobstore")
@Consumes(APPLICATION_JSON)
@Produces(APPLICATION_JSON)
@GZIP
@Api(tags = "Blobstore", description = "This resource is experimental and changes may be made to it in subsequent " +
        "releases that are not backwards compatible.")
@ApplicationScoped
@Logged
public class BlobStoreHandler extends MetricsServiceHandler {

    @POST
    @Path("/")
    @ApiOperation(value = "Create a store entry.", notes = "Clients are not required to explicitly create "
            + "an entry before storing data. Doing so however allows clients to prevent naming collisions and to "
            + "specify tags.")
    @ApiResponses(value = {
            @ApiResponse(code = 201, message = "Entry created successfully"),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 409, message = "Store entry with given key already exists", response = ApiError.class),
            @ApiResponse(code = 500, message = "entry creation failed due to an unexpected error",
                    response = ApiError.class)
    })
    public void createEntry(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(required = true) StoreEntry entry,
            @Context UriInfo uriInfo
    ) {
        URI location = uriInfo.getBaseUriBuilder().path("/store/{id}").build(entry.getKey());
        metricsService.createStoreEntry(getTenant(), entry).subscribe(new StoreEntryCreatedObserver(asyncResponse, location));
    }

    @GET
    @Path("/")
    @ApiOperation(value = "Find tenant's store entries.", response = StoreEntry.class, responseContainer = "List")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully retrieved at least one store entry."),
            @ApiResponse(code = 204, message = "No entry found."),
            @ApiResponse(code = 500, message = "Failed to retrieve entries due to unexpected error.", response = ApiError.class)
    })
    public void getEntries(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(value = "List of tags filters") @QueryParam("tags") String tags) {

        final Observable<StoreEntry> entriesObservable;
        if (tags != null) {
            entriesObservable = metricsService.findStoreEntriesWithFilters(getTenant(), tags);
        } else {
            entriesObservable = metricsService.findAllStoreEntries(getTenant());
        }

        entriesObservable
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> {
                    if (t instanceof PatternSyntaxException) {
                        asyncResponse.resume(badRequest(t));
                    } else {
                        asyncResponse.resume(serverError(t));
                    }
                });
    }

    @GET
    @Path("/{key}")
    @ApiOperation(value = "Retrieve single entry.", response = StoreEntry.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Blobstore entry was successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no entry is set."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching metric's definition.", response = ApiError.class) })
    public void getEntry(@Suspended final AsyncResponse asyncResponse, @PathParam("key") String key) {
        metricsService.findStoreEntry(getTenant(), key)
                .map(metric -> Response.ok(metric).build())
                .switchIfEmpty(Observable.just(ApiUtils.noContent()))
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @DELETE
    @Path("/{key}")
    @ApiOperation(value = "Deletes the store entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Deletion was successful."),
            @ApiResponse(code = 500, message = "Unexpected error occurred trying to delete the entry.")
    })
    public void deleteEntry(@Suspended AsyncResponse asyncResponse, @PathParam("key") String key) {
        metricsService.deleteStoreEntry(getTenant(), key).subscribe(new ResultSetObserver(asyncResponse));
    }

    @GET
    @Path("/tags/{tags}")
    @ApiOperation(value = "Retrieve blobstore's tag values", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Tags successfully retrieved."),
            @ApiResponse(code = 204, message = "No matching tags were found"),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching tags.", response = ApiError.class)
    })
    public void getTags(@Suspended final AsyncResponse asyncResponse,
            @ApiParam("Tag query") @PathParam("tags") Tags tags) {
        metricsService.getStoreTagValues(getTenant(), tags.getTags())
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @GET
    @Path("/{key}/tags")
    @ApiOperation(value = "Retrieve tags associated with the store entry.", response = Map.class)
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Entry's tags were successfully retrieved."),
            @ApiResponse(code = 204, message = "Query was successful, but no entry were found."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching entry's tags.", response = ApiError.class) })
    public void getEntryTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("key") String key) {
        metricsService.findStoreEntry(getTenant(), key)
                .map(StoreEntry::getTags)
                .map(ApiUtils::mapToResponse)
                .subscribe(asyncResponse::resume, t -> asyncResponse.resume(ApiUtils.serverError(t)));
    }

    @PUT
    @Path("/{key}/tags")
    @ApiOperation(value = "Update tags associated with the store entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Entry's tags were successfully updated."),
            @ApiResponse(code = 500, message = "Unexpected error occurred while updating entry's tags.", response = ApiError.class) })
    public void updateEntryTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("key") String key,
            @ApiParam(required = true) Map<String, String> tags) {
        metricsService.addStoreEntryTags(getTenant(), key, tags).subscribe(new ResultSetObserver(asyncResponse));
    }

    @DELETE
    @Path("/{key}/tags/{tags}")
    @ApiOperation(value = "Delete tags associated with the store entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Entry's tags were successfully deleted."),
            @ApiResponse(code = 400, message = "Invalid tags", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while trying to delete entry's tags.", response = ApiError.class) })
    public void deleteEntryTags(
            @Suspended final AsyncResponse asyncResponse,
            @PathParam("key") String key,
            @ApiParam(value = "Tag names", allowableValues = "Comma-separated list of tag names") @PathParam("tags") TagNames tags) {
        metricsService.deleteStoreEntryTags(getTenant(), key, tags.getNames()).subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/{key}/raw")
    @ApiOperation(value = "Set value for a single entry.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Setting data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void setData(
            @Suspended final AsyncResponse asyncResponse, @PathParam("key") String key,
            @ApiParam(value = "String value", required = true) String data
    ) {
        metricsService.setStoreData(getTenant(), Collections.singletonMap(key, data))
                .subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/raw")
    @ApiOperation(value = "Set values for multiple entries in a single call.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Setting data succeeded."),
            @ApiResponse(code = 400, message = "Missing or invalid payload", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error happened while storing the data",
                    response = ApiError.class)
    })
    public void addData(
            @Suspended final AsyncResponse asyncResponse,
            @ApiParam(value = "Map of key/values", required = true)
            Map<String, String> entries
    ) {
        metricsService.setStoreData(getTenant(), entries)
                .subscribe(new ResultSetObserver(asyncResponse));
    }

    @POST
    @Path("/raw/query")
    @ApiOperation(value = "Fetch raw data for multiple entries.")
    @ApiResponses(value = {
            @ApiResponse(code = 200, message = "Successfully fetched data."),
            @ApiResponse(code = 204, message = "Query was successful, but no data was found."),
            @ApiResponse(code = 400, message = "No entry key are specified", response = ApiError.class),
            @ApiResponse(code = 500, message = "Unexpected error occurred while fetching data.",
                    response = ApiError.class)
    })
    public void getData(
            @Suspended AsyncResponse asyncResponse,
            @ApiParam(required = true, value = "Query parameters that minimally must include a list of metric ids or " +
                    "tags. The standard start, end, order, and limit query parameters are supported as well.")
                    BlobStoreQueryRequest query) {

        if ((query.getKeys() == null || query.getKeys().isEmpty()) && query.getTags() == null) {
            asyncResponse.resume(ApiUtils.badRequest(new RuntimeApiError("Either keys or tags query parameters must be used")));
            return;
        }
        if (query.getKeys() != null && !query.getKeys().isEmpty() && query.getTags() != null) {
            asyncResponse.resume(ApiUtils.badRequest(new RuntimeApiError("Cannot use both the metrics and tags query parameters")));
            return;
        }

        final Observable<StoreEntry> entriesObservable;
        if (query.getKeys() != null) {
            entriesObservable = metricsService.findStoreEntries(getTenant(), query.getKeys());
        } else {
            entriesObservable = metricsService.findStoreEntriesWithFilters(getTenant(), query.getTags());
        }

        entriesObservable
                .toList()
                .map(ApiUtils::collectionToResponse)
                .subscribe(asyncResponse::resume, t -> {
                    if (t instanceof PatternSyntaxException) {
                        asyncResponse.resume(badRequest(t));
                    } else {
                        asyncResponse.resume(serverError(t));
                    }
                });
    }
}
