// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.web.responseprocessors;

import com.yahoo.bard.webservice.druid.client.FailureCallback;
import com.yahoo.bard.webservice.druid.client.HttpErrorCallback;
import com.yahoo.bard.webservice.druid.model.query.DruidAggregationQuery;

import com.fasterxml.jackson.databind.JsonNode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Response processor for partial data V2.
 * <p>
 * If Fili expects data that is not returned from Druid, it returns an error.
 */
public class PartialDataV2ResponseProcessor implements FullResponseProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(PartialDataV2ResponseProcessor.class);

    private final ResponseProcessor next;

    /**
     * Constructor.
     *
     * @param next  Next ResponseProcessor in the chain
     */
    public PartialDataV2ResponseProcessor(ResponseProcessor next) {
        this.next = next;
    }

    @Override
    public ResponseContext getResponseContext() {
        return next.getResponseContext();
    }

    @Override
    public FailureCallback getFailureCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getFailureCallback(druidQuery);
    }

    @Override
    public HttpErrorCallback getErrorCallback(DruidAggregationQuery<?> druidQuery) {
        return next.getErrorCallback(druidQuery);
    }

    @Override
    public void processResponse(JsonNode json, DruidAggregationQuery<?> query, LoggingContext metadata) {
        validateJsonResponse(json);

        int statusCode = json.get("status-code").asInt();
        if (statusCode == 200) {
            // implementation is blocked by https://github.com/yahoo/fili/pull/262
        }

        next.processResponse(json, query, metadata);
    }

    /**
     * Validates JSON response object to make sure it contains all of the following information.
     * <ul>
     *     <li>X-Druid-Response-Context</li>
     *     <ol>
     *         <li>uncoveredIntervals</li>
     *         <li>uncoveredIntervalsOverflowed</li>
     *     </ol>
     *     <li>status-code</li>
     * </ul>
     *
     * @param json  The JSON response that is to be validated
     */
    private static void validateJsonResponse(JsonNode json) {
        if (!json.has("X-Druid-Response-Context")) {
            logAndThrowRunTimeException("Response is missing X-Druid-Response-Context");
        }
        if (!json.get("X-Druid-Response-Context").has("uncoveredIntervals")) {
            logAndThrowRunTimeException("Response is missing \"uncoveredIntervals\" X-Druid-Response-Context");
        }
        if (!json.get("X-Druid-Response-Context").has("uncoveredIntervalsOverflowed")) {
            logAndThrowRunTimeException(
                    "Response is missing \"uncoveredIntervalsOverflowed\" X-Druid-Response-Context"
            );
        }
        if (!json.has("status-code")) {
            logAndThrowRunTimeException("Response is missing response status code");
        }
    }

    /**
     * Logs and throws RuntimeException with the provided error message.
     *
     * @param message  The error message passed to the logger and the exception.
     */
    private static void logAndThrowRunTimeException(String message) {
        LOG.error(message);
        throw new RuntimeException(message);
    }
}
