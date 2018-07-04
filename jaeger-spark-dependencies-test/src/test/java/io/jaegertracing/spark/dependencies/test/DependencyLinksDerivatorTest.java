/**
 * Copyright 2017 The Jaeger Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.jaegertracing.spark.dependencies.test;

import static org.junit.Assert.assertEquals;

import io.jaegertracing.spark.dependencies.test.rest.DependencyLink;
import io.jaegertracing.spark.dependencies.test.tree.Node;
import io.opentracing.mock.MockTracer;
import java.util.Arrays;
import java.util.Map;
import org.junit.Test;

/**
 * @author Pavol Loffay
 */
public class DependencyLinksDerivatorTest {

  @Test
  public void testRootToMap() {
    Node<MockTracingWrapper> root = new Node<>(new MockTracingWrapper(new MockTracer(), "foo", "bar"), null);
    new Node<>(new MockTracingWrapper(new MockTracer(), "child1", "op1"), root);
    new Node<>(new MockTracingWrapper(new MockTracer(), "child1", "op2"), root);
    new Node<>(new MockTracingWrapper(new MockTracer(), "child1", "op2"), root);
    new Node<>(new MockTracingWrapper(new MockTracer(), "child2", "op3"), root);
    Node<MockTracingWrapper> child3 = new Node<>(new MockTracingWrapper(new MockTracer(), "child3", "op4"), root);
    Node<MockTracingWrapper> child33 = new Node<>(new MockTracingWrapper(new MockTracer(), "child33", "op5"), child3);
    new Node<>(new MockTracingWrapper(new MockTracer(), "child333", "op6"), child33);

    Map<String, Map<String, Long>> depLinks = DependencyLinkDerivator.serviceDependencies(root);
    // 3 parents
    Map<String, Long> test = depLinks.get("foo-bar");
    assertEquals(3, depLinks.size());
    assertEquals(4, depLinks.get("foo-bar").size());
    assertEquals(1, depLinks.get("child3-op4").size());
    assertEquals(1, depLinks.get("child33-op5").size());

    assertEquals(Long.valueOf(2), depLinks.get("foo-bar").get("child1-op2"));
    assertEquals(Long.valueOf(1), depLinks.get("foo-bar").get("child2-op3"));
    assertEquals(Long.valueOf(1), depLinks.get("foo-bar").get("child3-op4"));
    assertEquals(Long.valueOf(1), depLinks.get("child3-op4").get("child33-op5"));
    assertEquals(Long.valueOf(1), depLinks.get("child33-op5").get("child333-op6"));
  }

  @Test
  public void testDepLinkToMap() {
    DependencyLink rootChild = new DependencyLink("root", "child", 3);
    DependencyLink childRoot = new DependencyLink("child", "root", 2);
    DependencyLink childChild2 = new DependencyLink("child", "child2", 6);

    Map<String, Map<String, Long>> depLinks = DependencyLinkDerivator.serviceDependencies(
        Arrays.asList(rootChild, childRoot, childChild2));

    assertEquals(2, depLinks.size());
    assertEquals(1, depLinks.get("root").size());
    assertEquals(2, depLinks.get("child").size());

    assertEquals(Long.valueOf(3), depLinks.get("root").get("child"));
    assertEquals(Long.valueOf(2), depLinks.get("child").get("root"));
    assertEquals(Long.valueOf(6), depLinks.get("child").get("child2"));
  }
}
