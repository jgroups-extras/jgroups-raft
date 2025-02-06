package org.jgroups.protocols.raft;

import java.util.Map;

import org.jgroups.logging.LogFactory;

/**
 * Legacy implementation of {@link Log} which was using LevelDB JNI preserved for compatibility. Implementation simply
 * delegated to {@link FileBasedLog}.
 *
 * @author Ugo Landini
 * @deprecated Delegates to {@link FileBasedLog}.
 */
@Deprecated
public class LevelDBLog extends FileBasedLog {

    protected final org.jgroups.logging.Log LOG = LogFactory.getLog(LevelDBLog.class);

    @Override
    public void init(String log_name, Map<String, String> args) throws Exception {
        LOG.warn("LevelDBLog log implementation is deprecated. Using implementation which delegates to FileBasedLog.");

        super.init(log_name, args);
    }
}
