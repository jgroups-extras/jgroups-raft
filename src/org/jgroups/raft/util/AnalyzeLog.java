package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.raft.blocks.CounterService;

import java.io.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;

/**
 * @author Bela Ban
 * @since  1.0.5
 */
public class AnalyzeLog {
    protected Class<? extends Log>       log_class=LevelDBLog.class;
    protected Function<LogEntry,String>  reader=CounterService::dumpLogEntry;
    protected Function<DataInput,String> snapshot_reader=CounterService::readAndDumpSnapshot;

    public AnalyzeLog logClass(String cl) throws ClassNotFoundException {
        log_class=(Class<? extends Log>)Class.forName(cl);
        return this;
    }

    public AnalyzeLog reader(String r) throws Exception {
        Method method=getMethod(r, LogEntry.class);
        reader=le -> invoke(method, le);
        return this;
    }

    public AnalyzeLog snapshotReader(String r) throws Exception {
        Method method=getMethod(r, DataInput.class);
        snapshot_reader=le -> invoke(method, le);
        return this;
    }

    protected static Method getMethod(String name, Class<?> param) throws Exception {
        int index=name.lastIndexOf(".");
        if(index < 0)
            throw new IllegalArgumentException(String.format("expected class.method (was %s)", name));
        String cl=name.substring(0, index), method_name=name.substring(index+1);
        Class<?> clazz=Class.forName(cl);
        return clazz.getMethod(method_name, param);
    }

    protected static String invoke(Method method, Object obj) {
        try {
            return (String)method.invoke(null, obj);
        }
        catch(Exception e) {
            return e.toString();
        }
    }

    protected void analzye(String ... paths) throws Exception {
        for(String path: paths)
            _analyze(path);
    }

    protected void _analyze(String path) throws Exception {
        try(Log l=createLog();DataInputStream snapshot=createSnapshotInput(path)) {
            l.init(path, null);
            long first=l.firstAppended(), commit=l.commitIndex(), last=l.lastAppended(), term=l.currentTerm();
            Address votedfor=l.votedFor();

            if(snapshot_reader != null && snapshot != null)
                System.out.printf("----------\nsnapshot: %s\n-----------\n", snapshot_reader.apply(snapshot));

            System.out.printf("first=%d, commit-index=%d, last-appended=%d, term=%d, voted-for=%s\n",
                              first, commit, last, term, votedfor);

            for(long i=Math.max(1, first); i <= last; i++) {
                StringJoiner sj=new StringJoiner(",");
                if(i == first)
                    sj.add("first");
                if(i == commit)
                    sj.add("commit-index");
                if(i == last)
                    sj.add("last-appended");
                LogEntry entry=l.get((int)i);
                if(entry != null) {
                    String log_contents=reader != null? reader.apply(entry) : entry.toString();
                    System.out.printf("%d [%d]: %s %s\n", i, entry.term(), log_contents, sj);
                }
            }
        }
    }

    protected Log createLog() throws Exception {
        return log_class.getDeclaredConstructor().newInstance();
    }

    protected static DataInputStream createSnapshotInput(String path) throws FileNotFoundException {int index=path.indexOf(".log");
        if(index < 0) return null;
        String snapshot_name=String.format("%s.snapshot", path.substring(0, index));
        return new File(snapshot_name).exists()? new DataInputStream(new FileInputStream(snapshot_name)) : null;
    }


    public static void main(String[] args) throws Exception {
        AnalyzeLog   al=new AnalyzeLog();
        List<String> files=new ArrayList<>();

        for(int i=0; i < args.length; i++) {
            if(args[i].startsWith("-log_class")) {
                al.logClass(args[++i]);
                continue;
            }
            if(args[i].equals("-reader")) {
                al.reader(args[++i]);
                continue;
            }
            if(args[i].startsWith("-snapshot_reader")) {
                al.snapshotReader(args[++i]);
                continue;
            }
            if(args[i].startsWith("-h")) {
                System.out.printf("%s [-log_class <log class>] [-reader <class.method>] " +
                                    "[-snapshot_reader <class.method>] [logfiles]\n",
                                  AnalyzeLog.class.getSimpleName());
                return;
            }
            files.add(args[i]);
        }
        String[] paths=files.toArray(new String[0]);
        al.analzye(paths);
    }
}
