package org.jgroups.raft.cli.probe.writer;

import org.jgroups.raft.cli.probe.ProbeResponseWriter;
import org.jgroups.raft.internal.probe.MetricCategory;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A decorating {@link ProbeResponseWriter} that filters metric responses to a single category.
 *
 * <p>
 * This writer rebuilds each {@link ProbeResponse} with a trimmed map containing only the requested {@link MetricCategory}
 * key, then forwards the filtered responses to the delegate writer. Identity fields ({@code raft-id}, {@code address}) are
 * preserved automatically since they are stored separately in the {@code ProbeResponse} object, not in the data map.
 * </p>
 *
 * @author José Bolina
 * @since 2.0
 */
public class CategoryFilterResponseWriter implements ProbeResponseWriter {

    private final ProbeResponseWriter delegate;
    private final MetricCategory category;

    public CategoryFilterResponseWriter(ProbeResponseWriter delegate, MetricCategory category) {
        this.delegate = delegate;
        this.category = category;
    }

    @Override
    public void accept(Collection<ProbeResponse> responses) {
        // Rebuild the responses with only the category.
        List<ProbeResponse> filtered = new ArrayList<>();

        for (ProbeResponse response : responses) {
            Map<String, Object> originalData = response.response();

            if (originalData.containsKey(category.key())) {
                Map<String, Object> trimmed = Map.of(category.key(), originalData.get(category.key()));
                filtered.add(new ProbeResponse(response.raftId(), response.source(), trimmed));
            }
        }

        delegate.accept(filtered);
    }

    @Override
    public ProbeResponseWriterFormat format() {
        return delegate.format();
    }

    @Override
    public PrintWriter out() {
        return delegate.out();
    }
}
