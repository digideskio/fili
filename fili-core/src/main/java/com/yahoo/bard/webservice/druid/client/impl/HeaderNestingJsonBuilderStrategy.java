// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.client.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingJsonFactory;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.asynchttpclient.Response;

import java.io.IOException;
import java.util.function.Function;

/**
 * A strategy that takes a JSON Node built from a Response, nests it and makes it a peer with the Response headers.
 */
public class HeaderNestingJsonBuilderStrategy implements Function<Response, JsonNode> {

    private final Function<Response, JsonNode> baseStrategy;

    /**
     * Constructor.
     *
     * @param baseStrategy  strategy to do initial JSON Node construction
     */
    public HeaderNestingJsonBuilderStrategy(Function<Response, JsonNode> baseStrategy) {
        this.baseStrategy = baseStrategy;
    }

    @Override
    public JsonNode apply(Response response) {
        ObjectNode objectNode = JsonNodeFactory.instance.objectNode();
        objectNode.set("response", baseStrategy.apply(response));
        try {
            objectNode.set(
                    "X-Druid-Response-Context",
                    new MappingJsonFactory()
                            .createParser(response.getHeader("X-Druid-Response-Context"))
                            .readValueAsTree()
            );
        } catch (IOException ioe) {
            throw new IllegalStateException(ioe);
        }
        return objectNode;
    }
}
