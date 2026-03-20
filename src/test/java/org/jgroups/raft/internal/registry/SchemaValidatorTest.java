package org.jgroups.raft.internal.registry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.jgroups.Global;
import org.jgroups.raft.internal.registry.SchemaEntry.Kind;
import org.jgroups.raft.internal.registry.SchemaEntry.Status;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SchemaValidatorTest {

    private Path tempDir;

    @BeforeMethod
    public void setUp() throws IOException {
        tempDir = Files.createTempDirectory("schema-validator-test");
    }

    @AfterMethod
    public void tearDown() throws IOException {
        if (tempDir != null && tempDir.toFile().exists()) {
            try (var walk = Files.walk(tempDir)) {
                walk.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
    }

    // First startup writes schema file.
    public void testFirstStartupWritesSchema() {
        List<SchemaEntry> current = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );

        SchemaValidator.validate(current, tempDir);

        File schemaFile = new File(tempDir.toFile(), "schema.raft");
        assertThat(schemaFile).exists();
    }

    // Subsequent startup with identical schema passes.
    public void testNoChangesPasses() {
        List<SchemaEntry> schema = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );

        SchemaValidator.validate(schema, tempDir);
        SchemaValidator.validate(schema, tempDir);
    }

    // Adding a new command is allowed.
    public void testNewCommandAllowed() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(2, 1, Kind.READ, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v2, tempDir);
    }

    // Changing parameter types of an existing command fails.
    public void testParameterTypesChangedFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.Integer"), Status.ACTIVE)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("parameter types changed");
    }

    // Changing kind (READ -> WRITE) fails.
    public void testKindChangedFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.READ, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("changed from READ to WRITE");
    }

    // Removing a lower version when a higher version exists is allowed.
    public void testRemovedWithHigherVersionAllowed() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "java.lang.Integer"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "java.lang.Integer"), Status.ACTIVE)
        );
        SchemaValidator.validate(v2, tempDir);
    }

    // Removing the highest version (downgrade) fails.
    public void testVersionDowngradeFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "java.lang.Integer"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    // Removing a command entirely without retiring it fails.
    public void testRemovedWithoutRetirementFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of();
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("retired");
    }

    // Retiring a previously active command is allowed.
    public void testRetiredCommandAllowed() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED)
        );
        SchemaValidator.validate(v2, tempDir);
    }

    // Retiring with wrong kind fails.
    public void testRetiredWrongKindFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.READ, List.of(), Status.RETIRED)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    // Retiring a command that never existed in the stored schema fails.
    public void testRetiredNonExistentFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(99, 1, Kind.WRITE, List.of(), Status.RETIRED)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("never registered");
    }

    // Missing schema file is treated as first startup.
    public void testMissingSchemaFileTreatedAsFirstStartup() {
        List<SchemaEntry> schema = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );

        SchemaValidator.validate(schema, tempDir);

        assertThat(new File(tempDir.toFile(), "schema.raft")).exists();
    }

    // A command that is both active and retired in the current schema fails.
    // This catches reuse of an (id, version) slot in both method annotations and retiredWrites/retiredReads.
    public void testActiveAndRetiredSimultaneouslyFails() {
        List<SchemaEntry> current = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(current, tempDir))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("declared both");
    }

    // Partial retirement of a multi-version command fails.
    // Retire v1 but remove v2 without retiring; highest version disappeared.
    public void testPartialRetirementFails() {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "java.lang.Integer"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        List<SchemaEntry> v2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(v2, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    // v1 -> add v2 -> remove v1 (superseded) -> retire all -> idempotent restart.
    public void testFullEvolutionLifecycle() {
        Collection<SchemaEntry> step1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(step1, tempDir);

        Collection<SchemaEntry> step2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "int"), Status.ACTIVE)
        );
        SchemaValidator.validate(step2, tempDir);

        Collection<SchemaEntry> step3 = List.of(
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "int"), Status.ACTIVE)
        );
        SchemaValidator.validate(step3, tempDir);

        Collection<SchemaEntry> step4 = List.of(
                new SchemaEntry(1, 2, Kind.WRITE, List.of(), Status.RETIRED)
        );
        SchemaValidator.validate(step4, tempDir);

        SchemaValidator.validate(step4, tempDir);
    }

    public void testMultipleCommandsEvolveIndependently() {
        Collection<SchemaEntry> step1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(2, 1, Kind.READ, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(step1, tempDir);

        Collection<SchemaEntry> step2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(1, 2, Kind.WRITE, List.of("java.lang.String", "int"), Status.ACTIVE),
                new SchemaEntry(2, 1, Kind.READ, List.of(), Status.RETIRED),
                new SchemaEntry(3, 1, Kind.READ, List.of("int"), Status.ACTIVE)
        );
        SchemaValidator.validate(step2, tempDir);
    }

    public void testRetiredEntriesPersistAcrossRestarts() {
        Collection<SchemaEntry> step1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE),
                new SchemaEntry(2, 1, Kind.READ, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(step1, tempDir);

        Collection<SchemaEntry> step2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED),
                new SchemaEntry(2, 1, Kind.READ, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(step2, tempDir);

        Collection<SchemaEntry> step3 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED),
                new SchemaEntry(2, 1, Kind.READ, List.of(), Status.RETIRED)
        );
        SchemaValidator.validate(step3, tempDir);
    }

    public void testEmptySchemaFileFailsWithClearError() throws IOException {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        Files.writeString(tempDir.resolve("schema.raft"), "");

        assertThatThrownBy(() -> SchemaValidator.validate(v1, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testCorruptedSchemaFileFailsWithClearError() throws IOException {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        Files.writeString(tempDir.resolve("schema.raft"), "{not valid json at all!!");

        assertThatThrownBy(() -> SchemaValidator.validate(v1, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testTruncatedSchemaFileFailsWithClearError() throws IOException {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        Path schemaFile = tempDir.resolve("schema.raft");
        String content = Files.readString(schemaFile);
        Files.writeString(schemaFile, content.substring(0, content.length() / 2));

        assertThatThrownBy(() -> SchemaValidator.validate(v1, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testUnsupportedSchemaVersionFails() throws IOException {
        List<SchemaEntry> v1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(v1, tempDir);

        Path schemaFile = tempDir.resolve("schema.raft");
        String content = Files.readString(schemaFile);
        Files.writeString(schemaFile, content.replace("\"version\": 1", "\"version\": 99"));

        assertThatThrownBy(() -> SchemaValidator.validate(v1, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }

    public void testCannotReuseRetiredSlot() {
        Collection<SchemaEntry> step1 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.String"), Status.ACTIVE)
        );
        SchemaValidator.validate(step1, tempDir);

        Collection<SchemaEntry> step2 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of(), Status.RETIRED)
        );
        SchemaValidator.validate(step2, tempDir);

        Collection<SchemaEntry> step3 = List.of(
                new SchemaEntry(1, 1, Kind.WRITE, List.of("java.lang.Integer"), Status.ACTIVE)
        );
        assertThatThrownBy(() -> SchemaValidator.validate(step3, tempDir))
                .isInstanceOf(IllegalStateException.class);
    }
}
