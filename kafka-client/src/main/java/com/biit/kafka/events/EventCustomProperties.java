package com.biit.kafka.events;

/*-
 * #%L
 * Kafka client
 * %%
 * Copyright (C) 2021 - 2025 BiiT Sourcing Solutions S.L.
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 * #L%
 */

public enum EventCustomProperties {

    ISSUER("issuer"),

    FACT_TYPE("factType"),

    //Label from the source.
    SOURCE_TAG("sourceTag"),

    //If it is a variable or parameter from source.
    PARAMETER_TAG("parameterTag"),

    //Deprecated, use organization event property.
    @Deprecated
    ORGANIZATION("organization");

    private final String tag;

    EventCustomProperties(String tag) {
        this.tag = tag;
    }

    public String getTag() {
        return tag;
    }
}
