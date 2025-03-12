package org.jgroups.protocols.raft;

import org.jgroups.logging.LogFactory;

import java.util.Map;

/**
 * Legacy implementation of {@link Log} which was using LevelDB JNI preserved for compatibility. Implementation simply
 * delegated to {@link FileBasedLog}.
 *
 * @author Ugo Landini
 * @deprecated Delegates to {@link FileBasedLog}.
 */
@Deprecated(since = "1.1.0", forRemoval = true)
public class LevelDBLog extends FileBasedLog {

    protected final org.jgroups.logging.Log LOG = LogFactory.getLog(LevelDBLog.class);

    @Override
    public void init(String log_name, Map<String, String> args) throws Exception {
        LOG.warn("LevelDBLog log implementation is deprecated. Verify the upgrade guide to migrate your data.");
        super.init(log_name, args);
    }
}
