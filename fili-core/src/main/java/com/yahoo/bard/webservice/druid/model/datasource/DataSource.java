// Copyright 2016 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.druid.model.datasource;

import com.yahoo.bard.webservice.data.config.names.TableName;
import com.yahoo.bard.webservice.druid.model.query.DruidQuery;
import com.yahoo.bard.webservice.table.ConstrainedTable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DataSource base class.
 */
public abstract class DataSource {

    private final DataSourceType type;
    private final ConstrainedTable physicalTable;

    /**
     * Constructor.
     *
     * @param type  Type of the data source
     * @param physicalTable  PhysicalTables pointed to by the DataSource
     */
    public DataSource(DataSourceType type, ConstrainedTable physicalTable) {
        this.type = type;
        this.physicalTable = physicalTable;
    }

    public DataSourceType getType() {
        return type;
    }

    /**
     * Get the data source physical table(s) as a collection.
     *
     * @return the set of physical tables for the data source
     *
     * @deprecated DataSources can only have a single table, and any unioning semantics is done at the PhysicalTable
     * level
     */
    @JsonIgnore
    @Deprecated
    public Set<ConstrainedTable> getPhysicalTables() {
        return Collections.singleton(getPhysicalTable());
    }

    /**
     * Get the data source physical table.
     *
     * @return the set of physical tables for the data source
     */
    @JsonIgnore
    public ConstrainedTable getPhysicalTable() {
        return physicalTable;
    }

    /**
     * Returns a set of identifiers used by Fili to identify this data source's physical tables.
     *
     * @return The set of names used by Fili to identify this data source's physical tables
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Set<String> getNames() {
        return Collections.unmodifiableSet((LinkedHashSet<String>) getPhysicalTable().getDataSourceNames().stream()
                .map(TableName::asName)
                .collect(Collectors.toCollection(LinkedHashSet::new))
        );
    }

    /**
     * Get the query that defines the data source.
     * <p>
     * May be null if the data source does not have a query.
     *
     * @return the query that this data source is generated from
     */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public DruidQuery<?> getQuery() {
        return null;
    }
}
