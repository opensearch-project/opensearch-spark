/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.core.model

/**
 * Create PIT (Point In Time) Request.
 *
 * @param indexName
 *   The name of the index.
 * @param keepAlive
 *   The amount of time to keep the PIT. Every time you access a PIT by using the Search API, the
 *   PIT lifetime is extended by the amount of time equal to the keep_alive parameter. The
 *   supported units are: <ul> <li>d - Days</li> <li>h - Hours</li> <li>m - Minutes</li> <li>s -
 *   Seconds</li> <li>ms - Milliseconds</li> <li>micros - Microseconds</li> <li>nanos -
 *   Nanoseconds</li> </ul>
 */
case class CreatePitReq(indexName: String, keepAlive: String)
