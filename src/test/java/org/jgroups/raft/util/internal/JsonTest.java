package org.jgroups.raft.util.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;

import java.time.Instant;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class JsonTest {

    public void testNullInputs() {
        assertThat(Json.toJson(null)).isEqualTo(Json.NULL);
        assertThat(Json.fromJson(null)).isNull();
        assertThat(Json.fromJson(Json.NULL)).isNull();
        assertThat(Json.flatten(null)).isNull();
    }

    public void testEmptyInputs() {
        assertThat(Json.toJson(Collections.emptyMap())).isEqualTo("{}");
        assertThat(Json.fromJson("{}")).isEqualTo(Collections.emptyMap());
        assertThat(Json.flatten(Collections.emptyMap())).isEqualTo(Collections.emptyMap());
    }

    public void testPrimitivesJson() {
        String expectedJson = """
{"a":1,"b":"b","c":0.5,"d":true}""";
        Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "b");
        expectedMap.put("c", 0.5);
        expectedMap.put("d", true);
        assertThat(Json.toJson(expectedMap)).isEqualTo(expectedJson);
        assertThat(Json.fromJson(expectedJson)).isEqualTo(expectedMap);
    }

    public void testComplexJson() {
        String expectedJson = """
{"a":1,"b":["a","b","c"],"c":{"a1":"larger \\nstring","b1":0.5,"c1":true},"d":false}""";
        Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", List.of("a", "b", "c"));
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("a1", "larger \nstring");
        nested.put("b1", 0.5);
        nested.put("c1", true);
        expectedMap.put("c", nested);
        expectedMap.put("d", false);
        assertThat(Json.toJson(expectedMap)).isEqualTo(expectedJson);
        assertThat(Json.fromJson(expectedJson)).isEqualTo(expectedMap);
    }

    public void testJsonWithNulls() {
        String expectedJson = """
{"a":1,"b":null}""";
        Map<String, Object> expectedMap = new LinkedHashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", null);
        assertThat(Json.toJson(expectedMap)).isEqualTo(expectedJson);
        assertThat(Json.fromJson(expectedJson)).isEqualTo(expectedMap);
    }

    public void testNestedJsonFlatten() {
        Map<String, Object> input = Map.of(
                "a", "root",
                "b", List.of(1, 2, 3),
                "c", Map.of("k1", "v1", "k2", "v2")
        );
        Map<String, String> expected = Map.of(
                "a", "root",
                "b[0]", "1",
                "b[1]", "2",
                "b[2]", "3",
                "c.k1", "v1",
                "c.k2", "v2"
        );

        assertThat(Json.flatten(input)).isEqualTo(expected);
    }

    public void testPrettyPrint() {
        Map<String, Object> input = new LinkedHashMap<>();
        input.put("name", "test");
        input.put("value", 42);
        input.put("col", List.of(1, 2));

        String result = Json.toPrettyJson(input);
        String nl = System.lineSeparator();
        String expected = """
{
  "name": "test",
  "value": 42,
  "col": [
    1,
    2
  ]
}""".replace("\n", nl);
        assertThat(result).contains(nl);
        assertThat(result).contains("  \"name\": \"test\"");
        assertThat(result).contains("  \"value\": 42");
        assertThat(result).isEqualTo(expected);
    }

    public void testParseWithWhitespace() {
        String nl = System.lineSeparator();
        String prettyJson = "{" + nl + "  \"name\": \"test\"," + nl + "  \"value\": 42" + nl + "}";
        Map<String, Object> result = Json.fromJson(prettyJson);

        assertThat(result).containsEntry("name", "test");
        assertThat(result).containsEntry("value", 42);
    }

    public void testPrettyPrintRoundTrip() {
        Map<String, Object> original = new LinkedHashMap<>();
        original.put("id", 1);
        original.put("items", List.of("a", "b", "c"));
        original.put("nested", Map.of("key", "value"));

        String pretty = Json.toPrettyJson(original);
        Map<String, Object> parsed = Json.fromJson(pretty);

        assertThat(parsed).containsEntry("id", 1);
        assertThat(parsed).containsKey("items");
        assertThat(parsed).containsKey("nested");
    }

    public void testInvalidInputs() {
        assertThatThrownBy(() -> Json.fromJson("")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Json.fromJson("{")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Json.fromJson("{\"a\":1")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> Json.fromJson("{\"a\":[1,2,3}")).isInstanceOf(IllegalStateException.class);

        // Complex object not supported by the serializer.
        record A(String key, String value) {}
        assertThatThrownBy(() -> Json.toJson(Map.of("a", new A("key", "value")))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> Json.toJson(Map.of("a", Instant.now()))).isInstanceOf(IllegalArgumentException.class);
    }
}
