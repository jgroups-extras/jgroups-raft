package org.jgroups.raft.util;

import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.LogEntry;

/**
 * @author Bela Ban
 * @since  1.0.5
 */
public class AnalyzeLog {

    protected static void analzye(String path) throws Exception {
        try(LevelDBLog l=new LevelDBLog()) {
            l.init(path, null);


            LogEntry entry=l.get(l.commitIndex());
            System.out.printf("entry at commit index %d: %s\n", l.commitIndex(), entry);

        }
    }

    public static void main(String[] args) throws Exception {
        String log_path="/tmp/A.log";

        for(int i=0; i < args.length; i++) {
            if("-f".equals(args[i])) {
                log_path=args[++i];
                continue;
            }
            System.out.printf("%s [-f logfile]\n", AnalyzeLog.class.getSimpleName());
            return;
        }

        AnalyzeLog.analzye(log_path);
    }
}
