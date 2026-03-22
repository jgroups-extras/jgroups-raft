package org.jgroups.raft.internal.statemachine;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;
import org.jgroups.raft.command.JGroupsRaftReadCommandOptions;
import org.jgroups.raft.command.JGroupsRaftWriteCommandOptions;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;

import org.testng.annotations.Test;

/**
 * Guards the {@link StateMachineWrapper} proxy array sizing.
 *
 * <p>
 * {@code ProxyWrapper} pre-builds one proxy per boolean combination of command options and indexes
 * into a fixed-size array using bit-packing. If someone adds or changes fields in the option
 * classes, these tests fail — signaling that {@code ProxyWrapper} must be updated to match.
 * </p>
 */
@Test(groups = Global.FUNCTIONAL)
public class ProxyWrapperArraySizeTest {

    public void testWriteProxyArrayCoversAllBooleanCombinations() {
        Field[] fields = instanceFields(JGroupsRaftWriteCommandOptions.WriteImpl.class);

        long booleanCount = Arrays.stream(fields).filter(f -> f.getType() == boolean.class).count();
        int expectedSize = 1 << booleanCount;

        assertThat(fields)
                .as("WriteImpl must only contain boolean fields; non-boolean fields require a different "
                        + "proxy caching strategy in StateMachineWrapper$ProxyWrapper")
                .allSatisfy(f -> assertThat(f.getType())
                        .as("field '%s'", f.getName())
                        .isEqualTo(boolean.class));

        assertThat(expectedSize)
                .as("WriteImpl has %d boolean fields requiring %d proxy slots; update ProxyWrapper.writes "
                        + "array size and submitWrite() index computation", booleanCount, expectedSize)
                .isEqualTo(2);
    }

    public void testReadProxyArrayCoversAllBooleanCombinations() {
        Field[] fields = instanceFields(JGroupsRaftReadCommandOptions.ReadImpl.class);

        long booleanCount = Arrays.stream(fields).filter(f -> f.getType() == boolean.class).count();
        int expectedSize = 1 << booleanCount;

        assertThat(fields)
                .as("ReadImpl must only contain boolean fields; non-boolean fields require a different "
                        + "proxy caching strategy in StateMachineWrapper$ProxyWrapper")
                .allSatisfy(f -> assertThat(f.getType())
                        .as("field '%s'", f.getName())
                        .isEqualTo(boolean.class));

        assertThat(expectedSize)
                .as("ReadImpl has %d boolean fields requiring %d proxy slots; update ProxyWrapper.reads "
                        + "array size and submitRead() index computation", booleanCount, expectedSize)
                .isEqualTo(4);
    }

    private static Field[] instanceFields(Class<?> clazz) {
        return Arrays.stream(clazz.getDeclaredFields())
                .filter(f -> !Modifier.isStatic(f.getModifiers()))
                .toArray(Field[]::new);
    }
}
