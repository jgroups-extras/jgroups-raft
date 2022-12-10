package org.jgroups.raft.util;

import org.jgroups.Address;
import org.jgroups.protocols.raft.LevelDBLog;
import org.jgroups.protocols.raft.Log;
import org.jgroups.protocols.raft.LogEntry;
import org.jgroups.protocols.raft.PersistentState;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.util.ByteArrayDataInputStream;

import java.io.DataInput;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
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
    protected PersistentState persistent_state;

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
        System.out.printf("\n---------- Analyze: %s ----------\n", path);
        try(Log l=createLog()) {
            l.init(path, null);
            long first=l.firstAppended(), commit=l.commitIndex(), last=l.lastAppended(), term=l.currentTerm();
            Address votedfor=l.votedFor();

            if(snapshot_reader != null) {
                ByteBuffer sn=l.getSnapshot();
                if(sn != null) {
                    persistent_state=new PersistentState();
                    DataInput snapshot=new ByteArrayDataInputStream(sn);
                    persistent_state.readFrom(snapshot);
                    System.out.printf("----------\npersistent state: \n%s\n-----------\n", persistent_state);
                    System.out.printf("----------\nsnapshot: %s\n-----------\n", snapshot_reader.apply(snapshot));
                }
            }

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
