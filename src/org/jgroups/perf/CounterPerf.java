package org.jgroups.perf;

import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.atomic.Counter;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.tests.perf.PerfUtil;
import org.jgroups.tests.perf.PerfUtil.Config;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.jgroups.util.Util.printTime;


/**
 * Tests performance of {@link org.jgroups.raft.blocks.CounterService}
 * @author Bela Ban
 * @version 1.0.5
 */
public class CounterPerf implements Receiver {
    private JChannel               channel;
    private Address                local_addr;
    private RpcDispatcher          disp;
    static final String            groupname="counter-perf";
    protected final List<Address>  members=new ArrayList<>();
    protected volatile View        view;
    protected volatile boolean     looping=true;
    protected ThreadFactory        thread_factory;

    protected CounterService       counter_service;
    protected Counter              counter;

    // ============ configurable properties ==================
    @Property protected int     num_threads=100;
    @Property protected int     time=30; // in seconds
    @Property protected boolean print_incrementers;
    @Property protected boolean print_details;
    @Property protected long    timeout=60000; // ms
    // ... add your own here, just don't forget to annotate them with @Property
    // =======================================================

    private static final Method[] METHODS=new Method[4];
    private static final short START                 =  0;
    private static final short GET_CONFIG            =  1;
    private static final short SET                   =  2;
    private static final short QUIT_ALL              =  3;

    protected static final Field NUM_THREADS, TIME, TIMEOUT, PRINT_INVOKERS, PRINT_DETAILS;


    protected static final String format=
      "[1] Start test [2] View [4] Threads (%d) [6] Time (%s)" +
        "\n[t] incr timeout (%s) [p] print counter" +
        "\n[d] print details (%b)  [i] print incrementers (%b)" +
        "\n[v] Version [x] Exit [X] Exit all\n";


    static {
        try {
            METHODS[START]      = CounterPerf.class.getMethod("startTest");
            METHODS[GET_CONFIG] = CounterPerf.class.getMethod("getConfig");
            METHODS[SET]        = CounterPerf.class.getMethod("set", String.class, Object.class);
            METHODS[QUIT_ALL]   = CounterPerf.class.getMethod("quitAll");

            NUM_THREADS=Util.getField(CounterPerf.class, "num_threads", true);
            TIME=Util.getField(CounterPerf.class, "time", true);
            TIMEOUT=Util.getField(CounterPerf.class, "timeout", true);
            PRINT_INVOKERS=Util.getField(CounterPerf.class, "print_incrementers", true);
            PRINT_DETAILS=Util.getField(CounterPerf.class, "print_details", true);
            PerfUtil.init();
            ClassConfigurator.addIfAbsent((short)1050, IncrementResult.class);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, String name, int bind_port, boolean use_fibers) throws Throwable {
        thread_factory=new DefaultThreadFactory("incrementer", false, true)
          .useFibers(use_fibers);
        if(use_fibers && Util.fibersAvailable())
            System.out.println("-- using fibers instead of threads");

        channel=new JChannel(props).setName(name);
        if(bind_port > 0) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setBindPort(bind_port);
        }

        disp=new RpcDispatcher(channel, this).setReceiver(this).setMethodLookup(id -> METHODS[id]);
        counter_service=new CounterService(channel).raftId(name).replTimeout(this.timeout);
        channel.connect(groupname);
        local_addr=channel.getAddress();

        if(members.size() < 2)
            return;
        Address coord=members.get(0);
        Config config=disp.callRemoteMethod(coord, new MethodCall(GET_CONFIG), new RequestOptions(ResponseMode.GET_ALL, 5000));
        if(config != null) {
            applyConfig(config);
            System.out.println("Fetched config from " + coord + ": " + config + "\n");
        }
        else
            System.err.println("failed to fetch config from " + coord);
    }

    void stop() {
        Util.close(disp, channel);
    }


    protected void stopEventLoop() {
        looping=false;
        Util.close(channel);
    }

    public void viewAccepted(View new_view) {
        this.view=new_view;
        System.out.println("** view: " + new_view);
        members.clear();
        members.addAll(new_view.getMembers());
    }


    // =================================== callbacks ======================================

    public IncrementResult startTest() throws Throwable {
        System.out.printf("running for %d seconds\n", time);
        final CountDownLatch latch=new CountDownLatch(1);
        counter=counter_service.getOrCreateCounter("counter", 0);

        Incrementer[] incrementers=new Incrementer[num_threads];
        Thread[]  threads=new Thread[num_threads];
        for(int i=0; i < threads.length; i++) {
            incrementers[i]=new Incrementer(latch);
            threads[i]=thread_factory.newThread(incrementers[i]);
            threads[i].setName("incrementer-" + (i+1));
            threads[i].start(); // waits on latch
        }

        long start=System.currentTimeMillis();
        latch.countDown();
        long interval=(long)((time * 1000.0) / 10.0);
        for(int i=1; i <= 10; i++) {
            Util.sleep(interval);
            System.out.printf("%d: %s\n", i, printAverage(start, incrementers));
        }

        for(Incrementer incrementer: incrementers)
            incrementer.stop();
        for(Thread t: threads)
            t.join();
        long total_time=System.currentTimeMillis() - start;

        System.out.println();
        AverageMinMax avg_incrs=null;
        for(int i=0; i < incrementers.length; i++) {
            Incrementer incrementer=incrementers[i];
            if(print_incrementers)
                System.out.printf("incrementer %s: increments %s\n", threads[i].getId(),
                                  print(incrementer.avg_incrtime, print_details));
            if(avg_incrs == null)
                avg_incrs=incrementer.avgIncrementTime();
            else
                avg_incrs.merge(incrementer.avgIncrementTime());
        }
        if(print_incrementers)
            System.out.printf("\navg over all incrementers: %s\n", print(avg_incrs, print_details));

        System.out.printf("\ndone (in %s ms)\n", total_time);
        return new IncrementResult(getTotalIncrements(incrementers), total_time, avg_incrs);
    }

    public void quitAll() {
        System.out.println("-- received quitAll(): shutting down");
        stopEventLoop();
        System.exit(0);
    }

    protected String printAverage(long start_time, Incrementer[] incrementers) {
        long tmp_time=System.currentTimeMillis() - start_time;
        long incrs=getTotalIncrements(incrementers);
        double incrs_sec=incrs / (tmp_time / 1000.0);
        return String.format("%,.0f increments/sec (%,d increments)", incrs_sec, incrs);
    }

    protected long getTotalIncrements(Incrementer[] incrementers) {
        long total=0;
        if(incrementers != null)
            for(Incrementer incr: incrementers)
                total+=incr.numIncrements();
        return total;
    }


    public void set(String field_name, Object value) {
        Field field=Util.getField(this.getClass(),field_name);
        if(field == null)
            System.err.println("Field " + field_name + " not found");
        else {
            Util.setField(field, this, value);
            System.out.println(field.getName() + "=" + value);
        }
    }


    public Config getConfig() {
        Config config=new Config();
        for(Field field: Util.getAllDeclaredFieldsWithAnnotations(CounterPerf.class, Property.class)) {
            if(field.isAnnotationPresent(Property.class)) {
                config.add(field.getName(), Util.getField(field, this));
            }
        }
        return config;
    }

    protected void applyConfig(Config config) {
        for(Map.Entry<String,Object> entry: config.values().entrySet()) {
            Field field=Util.getField(getClass(), entry.getKey());
            Util.setField(field, this, entry.getValue());
        }
    }

    // ================================= end of callbacks =====================================


    public void eventLoop() {
        while(looping) {
            try {
                int c=Util.keyPress(String.format(format, num_threads, Util.printTime(time, TimeUnit.MILLISECONDS),
                                                  Util.printTime(timeout, TimeUnit.MILLISECONDS),
                                                  print_details, print_incrementers));
                switch(c) {
                    case '1':
                        startBenchmark();
                        break;
                    case '2':
                        printView();
                        break;
                    case '4':
                        changeFieldAcrossCluster(NUM_THREADS, Util.readIntFromStdin("Number of incrementer threads: "));
                        break;
                    case '6':
                        changeFieldAcrossCluster(TIME, Util.readIntFromStdin("Time (secs): "));
                        break;
                    case 'd':
                        changeFieldAcrossCluster(PRINT_DETAILS, !print_details);
                        break;
                    case 'i':
                        changeFieldAcrossCluster(PRINT_INVOKERS, !print_incrementers);
                        break;
                    case 't':
                        changeFieldAcrossCluster(TIMEOUT, Util.readIntFromStdin("incr timeout (ms): "));
                        break;
                    case 'v':
                        System.out.printf("Version: %s, Java version: %s\n", Version.printVersion(),
                                          System.getProperty("java.vm.version", "n/a"));
                        break;
                    case 'p':
                        if(counter == null)
                            counter=counter_service.getOrCreateCounter("counter", 0);
                        long cnt=counter.get();
                        System.out.printf("current count: %d\n", cnt);
                        break;
                    case 'x':
                    case -1:
                        looping=false;
                        break;
                    case 'X':
                        try {
                            RequestOptions options=new RequestOptions(ResponseMode.GET_NONE, 0)
                              .flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
                            disp.callRemoteMethods(null, new MethodCall(QUIT_ALL), options);
                            break;
                        }
                        catch(Throwable t) {
                            System.err.println("Calling quitAll() failed: " + t);
                        }
                        break;
                    default:
                        break;
                }
            }
            catch(Throwable t) {
                t.printStackTrace();
            }
        }
        stop();
    }


    /** Kicks off the benchmark on all cluster nodes */
    void startBenchmark() throws Exception {
        RspList<IncrementResult> responses=null;
        try {
            RequestOptions options=new RequestOptions(ResponseMode.GET_ALL, 0)
              .flags(Message.Flag.OOB, Message.Flag.DONT_BUNDLE, Message.Flag.NO_FC);
            responses=disp.callRemoteMethods(null, new MethodCall(START), options);
        }
        catch(Throwable t) {
            System.err.println("starting the benchmark failed: " + t);
            return;
        }

        long total_incrs=0;
        long total_time=0;
        AverageMinMax avg_incrs=null;

        System.out.println("\n======================= Results: ===========================");
        for(Map.Entry<Address,Rsp<IncrementResult>> entry: responses.entrySet()) {
            Address mbr=entry.getKey();
            Rsp<IncrementResult> rsp=entry.getValue();
            IncrementResult result=rsp.getValue();
            if(result != null) {
                total_incrs+=result.num_increments;
                total_time+=result.total_time;
                if(avg_incrs == null)
                    avg_incrs=result.avg_increments;
                else
                    avg_incrs.merge(result.avg_increments);
            }
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec=total_incrs / ( total_time/ 1000.0);
        System.out.println("\n");
        System.out.println(Util.bold(String.format("Throughput: %,.2f increments/sec/node\n" +
                                                   "Time:       %s / increment\n",
                                                   total_reqs_sec, print(avg_incrs, print_details))));
        System.out.println("\n\n");
    }
    

    protected void changeFieldAcrossCluster(Field field, Object value) throws Exception {
        disp.callRemoteMethods(null, new MethodCall(SET, field.getName(), value), RequestOptions.SYNC());
    }


    protected void printView() {
        System.out.printf("\n-- local: %s, view: %s\n", local_addr, view);
        try {
            System.in.skip(System.in.available());
        }
        catch(Exception ignored) {
        }
    }

    protected static String print(AverageMinMax avg, boolean details) {
        return details? String.format("min/avg/max = %s", avg.toString(TimeUnit.NANOSECONDS)) :
          String.format("%s", Util.printTime(avg.average(), TimeUnit.NANOSECONDS));
    }



    protected class Incrementer implements Runnable {
        private final CountDownLatch latch;
        private long                 num_increments;
        private final AverageMinMax  avg_incrtime=new AverageMinMax(); // in ns
        private volatile boolean     running=true;


        public Incrementer(CountDownLatch latch) {
            this.latch=latch;
        }

        public long          numIncrements()    {return num_increments;}
        public AverageMinMax avgIncrementTime() {return avg_incrtime;}
        public void          stop()             {running=false;}

        public void run() {
            try {
                latch.await();
            }
            catch(InterruptedException e) {
                e.printStackTrace();
            }

            while(running) {
                try {
                    long start=System.nanoTime();
                    counter.incrementAndGet();
                    long incr_time=System.nanoTime()-start;
                    avg_incrtime.add(incr_time);
                    num_increments++;
                }
                catch(Throwable t) {
                    if(running)
                        t.printStackTrace();
                }
            }
        }

    }

    protected static class IncrementResult implements Streamable {
        protected long          num_increments;
        protected long          total_time;     // in ms
        protected AverageMinMax avg_increments; // in ns

        public IncrementResult() {
        }

        public IncrementResult(long num_increments, long total_time, AverageMinMax avg_increments) {
            this.num_increments=num_increments;
            this.total_time=total_time;
            this.avg_increments=avg_increments;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(num_increments, out);
            Bits.writeLongCompressed(total_time, out);
            Util.writeStreamable(avg_increments, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_increments=Bits.readLongCompressed(in);
            total_time=Bits.readLongCompressed(in);
            avg_increments=Util.readStreamable(AverageMinMax::new, in);
        }

        public String toString() {
            double total_reqs_per_sec=num_increments / (total_time / 1000.0);
            return String.format("%,.2f increments/sec (%,d increments, %s / increment)",
                                 total_reqs_per_sec, num_increments,
                                 Util.printTime(avg_increments.average(), TimeUnit.NANOSECONDS));
        }
    }

    // todo: copied from JGroups; remove when 5.2.1.Final is used
    public static class AverageMinMax extends Average {
        protected long       min=Long.MAX_VALUE, max=0;
        protected List<Long> values;

        public long          min()                        {return min;}
        public long          max()                        {return max;}
        public boolean       usePercentiles()             {return values != null;}
        public AverageMinMax usePercentiles(int capacity) {values=capacity > 0? new ArrayList<>(capacity) : null; return this;}

        public <T extends Average> T add(long num) {
            super.add(num);
            min=Math.min(min, num);
            max=Math.max(max, num);
            if(values != null)
                values.add(num);
            return (T)this;
        }

        public <T extends Average> T merge(T other) {
            if(other.count() == 0)
                return (T)this;
            super.merge(other);
            if(other instanceof AverageMinMax) {
                AverageMinMax o=(AverageMinMax)other;
                this.min=Math.min(min, o.min());
                this.max=Math.max(max, o.max());
                if(this.values != null)
                    this.values.addAll(o.values);
            }
            return (T)this;
        }

        public void clear() {
            super.clear();
            if(values != null)
                values.clear();
            min=Long.MAX_VALUE; max=0;
        }

        public String percentiles() {
            if(values == null) return "n/a";
            Collections.sort(values);
            double stddev=stddev();
            return String.format("stddev: %.2f, 50: %d, 90: %d, 99: %d, 99.9: %d, 99.99: %d, 99.999: %d, 100: %d\n",
                                 stddev, p(50), p(90), p(99), p(99.9), p(99.99), p(99.999), p(100));
        }

        public String toString() {
            return count == 0? "n/a" : String.format("min/avg/max=%,d/%,.2f/%,d", min, getAverage(), max);
        }

        public String toString(TimeUnit u) {
            if(count == 0)
                return "n/a";
            return String.format("%s/%s/%s", printTime(min, u), printTime(getAverage(), u), printTime(max, u));
        }

        public void writeTo(DataOutput out) throws IOException {
            super.writeTo(out);
            Bits.writeLongCompressed(min, out);
            Bits.writeLongCompressed(max, out);
        }

        public void readFrom(DataInput in) throws IOException {
            super.readFrom(in);
            min=Bits.readLongCompressed(in);
            max=Bits.readLongCompressed(in);
        }


        protected long p(double percentile) {
            if(values == null)
                return -1;
            int size=values.size();
            int index=(int)(size * (percentile/100.0));
            return values.get(index-1);
        }

        protected double stddev() {
            if(values == null) return -1.0;
            double av=average();
            int size=values.size();
            double variance=values.stream().map(v -> (v - av)*(v - av)).reduce(0.0, Double::sum) / size;
            return Math.sqrt(variance);
        }


    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String  props=null, name=null;
        boolean run_event_loop=true, use_fibers=true;
        int port=0;

        for(int i=0; i < args.length; i++) {
            if("-props".equals(args[i])) {
                props=args[++i];
                continue;
            }
            if("-name".equals(args[i])) {
                name=args[++i];
                continue;
            }
            if("-nohup".equals(args[i])) {
                run_event_loop=false;
                continue;
            }
            if("-port".equals(args[i])) {
                port=Integer.parseInt(args[++i]);
                continue;
            }
            if("-use_fibers".equals(args[i])) {
                use_fibers=Boolean.parseBoolean(args[++i]);
                continue;
            }
            help();
            return;
        }

        if(name == null)
            throw new IllegalArgumentException("name (raft-id) must be set");

        CounterPerf test=null;
        try {
            test=new CounterPerf();
            test.init(props, name, port, use_fibers);
            if(run_event_loop)
                test.eventLoop();
            else {
                for(;;)
                    Util.sleep(60000);
            }
        }
        catch(Throwable ex) {
            ex.printStackTrace();
            if(test != null)
                test.stop();
        }
    }

    static void help() {
        System.out.println("CounterPerf [-props <props>] [-name name] [-nohup] [-port <bind port>] " +
                             "[-use_fibers <true|false>]");
    }


}