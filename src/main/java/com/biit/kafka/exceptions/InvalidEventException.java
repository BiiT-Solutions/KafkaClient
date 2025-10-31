package com.biit.kafka.exceptions;

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

public class InvalidEventException extends RuntimeException {

    public InvalidEventException(Class<?> clazz, String message) {
        super(clazz + ": " + message);
    }

    public InvalidEventException(Class<?> clazz, String message, Throwable e) {
        super(clazz + ": " + message, e);
    }

    public InvalidEventException(Class<?> clazz, Throwable e) {
        super(clazz.getName(), e);
    }
}
