// Copyright 2017 Yahoo Inc.
// Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.
package com.yahoo.bard.webservice.table.availability;

import com.yahoo.bard.webservice.data.config.names.DataSourceName;
import com.yahoo.bard.webservice.metadata.DataSourceMetadataService;
import com.yahoo.bard.webservice.table.resolver.PhysicalDataSourceConstraint;
import com.yahoo.bard.webservice.util.SimplifiedIntervalList;

import java.util.Objects;
import java.util.Set;

import javax.validation.constraints.NotNull;

/**
 * An availability that provides column and table available interval services for concrete physical table.
 */
public class ConcreteAvailability extends BaseMetadataAvailability {
    /**
     * Constructor.
     *
     * @param dataSourceName  The name of the data source associated with this Availability
     * @param metadataService  A service containing the datasource segment data
     */
    public ConcreteAvailability(
            @NotNull DataSourceName dataSourceName,
            @NotNull DataSourceMetadataService metadataService
    ) {
        super(dataSourceName, metadataService);
    }

    @Override
    public SimplifiedIntervalList getAvailableIntervals(PhysicalDataSourceConstraint constraint) {

        Set<String> requestColumns = constraint.getAllColumnPhysicalNames();
        if (requestColumns.isEmpty()) {
            return new SimplifiedIntervalList();
        }

        // Need to ensure requestColumns is not empty in order to prevent returning null by reduce operation
        return requestColumns.stream()
                .map(physicalName -> getAllAvailableIntervals().getOrDefault(
                        physicalName,
                        new SimplifiedIntervalList()
                ))
                .reduce(SimplifiedIntervalList::intersect)
                .orElse(new SimplifiedIntervalList());
    }

    @Override
    public String toString() {
        return String.format("ConcreteAvailability for data source: %s", getDataSourceName().asName());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ConcreteAvailability) {
            ConcreteAvailability that = (ConcreteAvailability) obj;
            return Objects.equals(getDataSourceName().asName(), that.getDataSourceName().asName())
                    // Since metadata service is mutable, use instance equality to ensure table equality is stable
                    && getDataSourceMetadataService() == that.getDataSourceMetadataService();
        }
        return false;
    }

    @Override
    public int hashCode() {
        // Leave metadataService out of hash because it is mutable
        return Objects.hash(getDataSourceName());
    }
}
