package org.jgroups.raft.internal.serialization.binary;

import static org.assertj.core.api.Assertions.assertThat;

import org.jgroups.Global;

import org.testng.annotations.Test;

@Test(groups = Global.FUNCTIONAL, singleThreaded = true)
public class SerializationSizeCacheTest {

    @Test
    public void testLookupMissReturnsMinusOne() {
        SerializationSizeCache cache = new SerializationSizeCache();

        int result = cache.lookup(String.class);

        assertThat(result).isEqualTo(-1);
    }

    @Test
    public void testLookupHitReturnsSize() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Update with a size
        cache.update(String.class, 100);

        int result = cache.lookup(String.class);

        // Should return 100 + 10% padding = 110
        assertThat(result).isEqualTo(110);
    }

    @Test
    public void testEagerIncreaseWhenActualSizeIsLarger() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Initial update: 100 bytes → cached as 110 (100 + 10%)
        cache.update(String.class, 100);
        assertThat(cache.lookup(String.class)).isEqualTo(110);

        // Larger size: 200 bytes → should eagerly increase to 220 (200 + 10%)
        cache.update(String.class, 200);

        assertThat(cache.lookup(String.class)).isEqualTo(220);
    }

    @Test
    public void testSlowDecreaseWhenActualSizeIsSmaller() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Initial update: 1000 bytes → cached as 1100 (1000 + 10%)
        cache.update(String.class, 1000);
        assertThat(cache.lookup(String.class)).isEqualTo(1100);

        // Smaller size: 500 bytes → slow decrease: (1100 * 9 + 500) / 10 = 1040
        cache.update(String.class, 500);

        assertThat(cache.lookup(String.class)).isEqualTo(1040);
    }

    @Test
    public void testNoUpdateWhenSizeIsEqual() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Initial update: 100 bytes → cached as 110
        cache.update(String.class, 100);
        int initialSize = cache.lookup(String.class);

        // Update with size that equals cached value minus padding: 110
        // Actual: 110, Cached: 110 → no change
        cache.update(String.class, 110);

        assertThat(cache.lookup(String.class)).isEqualTo(initialSize);
    }

    @Test
    public void testRoundRobinEvictionWhenCacheFull() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Fill cache with 8 entries (cache size = 8)
        Class<?>[] classes = {
            String.class,
            Integer.class,
            Long.class,
            Double.class,
            Float.class,
            Boolean.class,
            Character.class,
            Byte.class
        };

        for (int i = 0; i < classes.length; i++) {
            cache.update(classes[i], 100 * (i + 1));
        }

        // Verify all 8 are cached
        for (int i = 0; i < classes.length; i++) {
            assertThat(cache.lookup(classes[i]))
                .isEqualTo(110 * (i + 1)); // 100*(i+1) + 10%
        }

        // Add 9th entry - should evict String.class (first inserted)
        cache.update(Short.class, 900);

        // String.class should now be evicted
        assertThat(cache.lookup(String.class)).isEqualTo(-1);

        // Short.class should be cached
        assertThat(cache.lookup(Short.class)).isEqualTo(990); // 900 + 10%

        // All others should still be there
        for (int i = 1; i < classes.length; i++) {
            assertThat(cache.lookup(classes[i]))
                .isEqualTo(110 * (i + 1));
        }
    }

    @Test
    public void testIdentityBasedClassComparison() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Use the same Class instance
        Class<?> stringClass1 = String.class;
        Class<?> stringClass2 = String.class;

        cache.update(stringClass1, 100);

        // Should find it with the same class (identity comparison)
        assertThat(cache.lookup(stringClass2)).isEqualTo(110);
    }

    @Test
    public void testMultipleUpdatesFollowAdaptiveLogic() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Start: 1000 → cached 1100
        cache.update(Integer.class, 1000);
        assertThat(cache.lookup(Integer.class)).isEqualTo(1100);

        // Increase: 1500 → cached 1650 (eager)
        cache.update(Integer.class, 1500);
        assertThat(cache.lookup(Integer.class)).isEqualTo(1650);

        // Decrease: 1000 → cached (1650 * 9 + 1000) / 10 = 1585 (slow)
        cache.update(Integer.class, 1000);
        assertThat(cache.lookup(Integer.class)).isEqualTo(1585);

        // Decrease again: 1000 → (1585 * 9 + 1000) / 10 = 1526 (slow)
        cache.update(Integer.class, 1000);
        assertThat(cache.lookup(Integer.class)).isEqualTo(1526);
    }

    @Test
    public void testZeroSizeHandling() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Update with size 0
        cache.update(String.class, 0);

        // Should cache 0 + 0/10 = 0
        assertThat(cache.lookup(String.class)).isEqualTo(0);
    }

    @Test
    public void testVeryLargeSize() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Update with very large size
        int largeSize = 100_000_000; // 100 MB
        cache.update(String.class, largeSize);

        // Should cache largeSize + 10% = 110_000_000
        assertThat(cache.lookup(String.class)).isEqualTo(110_000_000);
    }

    @Test
    public void testDifferentClassesIndependentlyCached() {
        SerializationSizeCache cache = new SerializationSizeCache();

        cache.update(String.class, 100);
        cache.update(Integer.class, 200);
        cache.update(Long.class, 300);

        assertThat(cache.lookup(String.class)).isEqualTo(110);
        assertThat(cache.lookup(Integer.class)).isEqualTo(220);
        assertThat(cache.lookup(Long.class)).isEqualTo(330);
    }

    @Test
    public void testUpdateAfterEvictionCreatesNewEntry() {
        SerializationSizeCache cache = new SerializationSizeCache();

        // Fill cache
        for (int i = 0; i < 8; i++) {
            cache.update(getClassByIndex(i), 100);
        }

        // Evict first entry by adding 9th
        cache.update(getClassByIndex(8), 200);

        // First entry should be evicted
        assertThat(cache.lookup(getClassByIndex(0))).isEqualTo(-1);

        // Re-add first entry - should be inserted as new (at slot 1 now)
        cache.update(getClassByIndex(0), 500);

        assertThat(cache.lookup(getClassByIndex(0))).isEqualTo(550); // 500 + 10%
    }

    /**
     * Helper to get different class instances for testing.
     */
    private Class<?> getClassByIndex(int index) {
        return switch (index) {
            case 0 -> String.class;
            case 1 -> Integer.class;
            case 2 -> Long.class;
            case 3 -> Double.class;
            case 4 -> Float.class;
            case 5 -> Boolean.class;
            case 6 -> Character.class;
            case 7 -> Byte.class;
            case 8 -> Short.class;
            default -> Object.class;
        };
    }
}
