/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.examples.datagrid;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityKey;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.query.IndexQuery;
import org.apache.ignite.cache.query.IndexQueryCriterion;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.examples.ExampleNodeStartup;
import org.apache.ignite.examples.model.Organization;
import org.apache.ignite.examples.model.Person;


import java.io.Serializable;
import java.util.Set;

import static org.apache.ignite.cache.query.IndexQueryCriteriaBuilder.*;

/**
 * Cache queries example. This example demonstrates TEXT, FULL SCAN and INDEX
 * queries over cache.
 * <p>
 * Example also demonstrates usage of fields queries that return only required
 * fields instead of whole key-value pairs. When fields queries are distributed
 * across several nodes, they may not work as expected. Keep in mind following
 * limitations (not applied if data is queried from one node only):
 * <ul>
 *     <li>
 *         Non-distributed joins will work correctly only if joined objects are stored in
 *         collocated mode. Refer to {@link AffinityKey} javadoc for more details.
 *         <p>
 *         To use distributed joins it is necessary to set query 'distributedJoin' flag using
 *         {@link SqlFieldsQuery#setDistributedJoins(boolean)}.
 *     </li>
 *     <li>
 *         Note that if you created query on to replicated cache, all data will
 *         be queried only on one node, not depending on what caches participate in
 *         the query (some data from partitioned cache can be lost). And visa versa,
 *         if you created it on partitioned cache, data from replicated caches
 *         will be duplicated.
 *     </li>
 * </ul>
 * <p>
 * Remote nodes should be started using {@link ExampleNodeStartup} which will
 * start node with {@code examples/config/example-ignite.xml} configuration.
 */
public class CacheQueryExample {

    private static final String PERSON_CACHE = CacheQueryExample.class.getSimpleName() + "Persons";

    public static final class PersonKey implements Serializable {

        private final long id;
        @AffinityKeyMapped
        @QuerySqlField(orderedGroups = @QuerySqlField.Group(name = "a_idx", order = 0))
        private final long orgId;

        public PersonKey(long id, long organizationId) {
            this.id = id;
            this.orgId = organizationId;
        }

        @Override
        public String toString() {
            return "PersonKey{" +
                    "id=" + id +
                    ", orgId=" + orgId +
                    '}';
        }
    }

    public static final class Person implements Serializable {

        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "a_idx", order = 1), @QuerySqlField.Group(name = "b_idx", order = 1)})
        public String role;
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "a_idx", order = 2), @QuerySqlField.Group(name = "b_idx", order = 2)})
        public String other;
        @QuerySqlField(orderedGroups = {@QuerySqlField.Group(name = "a_idx", order = 3), @QuerySqlField.Group(name = "b_idx", order = 3)})
        public long salary;

        public Person(long salary, String role, String other) {
            this.salary = salary;
            this.role = role;
            this.other = other;
        }

        @Override
        public String toString() {
            return "Person{" +
                    "role='" + role + '\'' +
                    ", other='" + other + '\'' +
                    ", salary=" + salary +
                    '}';
        }
    }

    public static void main(String[] args) throws Exception {
        try (final var ignite = Ignition.start("examples/config/example-ignite.xml")) {
            try {
                ignite.getOrCreateCache(new CacheConfiguration<>(PERSON_CACHE).setIndexedTypes(PersonKey.class, Person.class));
                initialize();
                indexQuery();
            }
            finally {
                ignite.destroyCache(PERSON_CACHE);
            }
        }
    }

    private static void indexQuery() {
        final var cache = Ignition.ignite().<PersonKey, Person>cache(PERSON_CACHE);

        doQuery(cache, "orgId >= 0, role[a,b], other[x]",  gte("orgId", 0L), in("role", Set.of("a", "b")), in("other", Set.of("x"))); // WRONG
        doQuery(cache, "orgId >= 0, role[a], other[x]",    gte("orgId", 0L), in("role", Set.of("a")),      in("other", Set.of("x"))); // WRONG
        doQuery(cache, "orgId >= 0, role[b], other[x]",    gte("orgId", 0L), in("role", Set.of("b")),      in("other", Set.of("x"))); // WRONG
        doQuery(cache, "orgId >= 0, role[a,b], other = x", gte("orgId", 0L), in("role", Set.of("a", "b")), eq("other", "x")); // WRONG

        doQuery(cache, "orgId >= 0, role[a]",                      gte("orgId", 0L),        in("role", Set.of("a")));
        doQuery(cache, "orgId >= 0, role = a, other = x",          gte("orgId", 0L),        eq("role", "a"),       eq("other", "x"));
        doQuery(cache, "orgId[1,2], role = a, other = x",          in("orgId", Set.of(1L, 2L)), eq("role", "a"),       eq("other", "x"));
        doQuery(cache, "orgId[1,2], role[a], other[x]",            in("orgId", Set.of(1L, 2L)), in("role", Set.of("a")),   in("other", Set.of("x")));
        doQuery(cache, "orgId[1,2], role[a], other[x], salary[1]", in("orgId", Set.of(1L, 2L)), in("role", Set.of("a")),   in("other", Set.of("x")),   in("salary", Set.of(1)));

        doQuery(cache, "role[b], other[y], salary > 1", in("role", Set.of("b")),     in("other", Set.of("y")), gt("salary", 1));
        doQuery(cache, "role[a], other[x]",             in("role", Set.of("a")),     in("other", Set.of("x")));
        doQuery(cache, "role[b], other[y]",             in("role", Set.of("b")),     in("other", Set.of("y")));
        doQuery(cache, "role[b], other[x]",             in("role", Set.of("b")),     in("other", Set.of("x")));
        doQuery(cache, "role[a,b], other[x]",           in("role", Set.of("a","b")), in("other", Set.of("x")));
    }

    private static void doQuery(IgniteCache<PersonKey, Person> cache, String msg, IndexQueryCriterion... criteria) {
        var query = cache.query(new IndexQuery<PersonKey, Person>(Person.class).setCriteria(criteria));
        print(msg, query.getAll());
    }

    private static void initialize() {
        IgniteCache<PersonKey, Person> pc2 = Ignition.ignite().cache(PERSON_CACHE);
        pc2.put(new PersonKey(1, 1), new Person(1, "a", "x"));
        pc2.put(new PersonKey(2, 1), new Person(1, "a", "y"));
        pc2.put(new PersonKey(3, 1), new Person(1, "b", "y"));
        pc2.put(new PersonKey(4, 1), new Person(1, "b", "x"));
        pc2.put(new PersonKey(5, 1), new Person(2, "a", "x"));
        pc2.put(new PersonKey(6, 1), new Person(2, "a", "y"));
        pc2.put(new PersonKey(7, 1), new Person(2, "b", "y"));
        pc2.put(new PersonKey(8, 1), new Person(2, "b", "x"));

        pc2.put(new PersonKey( 9, 2), new Person(1, "a", "x"));
        pc2.put(new PersonKey(10, 2), new Person(1, "a", "y"));
        pc2.put(new PersonKey(11, 2), new Person(1, "b", "y"));
        pc2.put(new PersonKey(12, 2), new Person(1, "b", "x"));
        pc2.put(new PersonKey(13, 2), new Person(2, "a", "x"));
        pc2.put(new PersonKey(14, 2), new Person(2, "a", "y"));
        pc2.put(new PersonKey(15, 2), new Person(2, "b", "y"));
        pc2.put(new PersonKey(16, 2), new Person(2, "b", "x"));
    }

    /**
     * Prints message and query results.
     *
     * @param msg Message to print before all objects are printed.
     * @param col Query results.
     */
    private static void print(String msg, Iterable<?> col) {
        print(msg);
        print(col);
    }

    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        System.out.println();
        System.out.println(">>> " + msg);
    }

    /**
     * Prints query results.
     *
     * @param col Query results.
     */
    private static void print(Iterable<?> col) {
        for (Object next : col)
            System.out.println(">>>     " + next);
    }
}
