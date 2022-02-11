package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.LogEntry;

import java.util.StringJoiner;

/**
 * @author Bela Ban
 * @since  1.0.5
 */
public class AnalyzeLog {

    protected static void analzye(String path) throws Exception {
        try(LevelDBLog l=new LevelDBLog()) {
            l.init(path, null);

            long first=l.firstAppended(), commit=l.commitIndex(), last=l.lastAppended(), term=l.currentTerm();
            Address votedfor=l.votedFor();

            System.out.printf("first=%d, commit-index=%d, last-appended=%d, term=%d, voted-for=%s\n",
                              first, commit, last, term, votedfor);

            for(long i=first; i <= last; i++) {
                StringJoiner sj=new StringJoiner(",");
                if(i == first)
                    sj.add("first");
                if(i == commit)
                    sj.add("commit-index");
                if(i == last)
                    sj.add("last-appended");
                LogEntry entry=l.get((int)i);
                System.out.printf("%d: %s %s\n", i, entry, sj);
            }

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
