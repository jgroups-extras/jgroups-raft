package org.jgroups.perf.counter;

import org.HdrHistogram.AbstractHistogram;
import org.HdrHistogram.Histogram;
import org.jgroups.*;
import org.jgroups.annotations.Property;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.ResponseMode;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.TP;
import org.jgroups.raft.blocks.CounterService;
import org.jgroups.raft.blocks.RaftCounter;
import org.jgroups.tests.perf.PerfUtil;
import org.jgroups.tests.perf.PerfUtil.Config;
import org.jgroups.util.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;


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
    protected RaftCounter          counter;
    private volatile String        histogramPath;

    // ============ configurable properties ==================
    @Property protected int     num_threads=100;
    @Property protected int     time=30; // in seconds
    @Property protected boolean print_updaters;
    @Property protected boolean print_details;
    @Property protected long    timeout=60000; // ms
    @Property protected int     range=10;
    @Property protected String  benchmark="sync";
    // ... add your own here, just don't forget to annotate them with @Property
    // =======================================================

    private static final Method[] METHODS=new Method[4];
    private static final short START                 =  0;
    private static final short GET_CONFIG            =  1;
    private static final short SET                   =  2;
    private static final short QUIT_ALL              =  3;

    protected static final Field NUM_THREADS, TIME, TIMEOUT, PRINT_INVOKERS, PRINT_DETAILS, RANGE, BENCHMARK;
    protected static final String COUNTER="counter";

    protected static final String format=
      "[1] Start test [2] View [4] Threads (%d) [6] Time (%s) [r] Range (%d)" +
        "\n[t] incr timeout (%s) [d] details (%b)  [i] print updaters (%b)" +
        "\n[b] benchmark mode (%s)" +
        "\n[v] Version [x] Exit [X] Exit all %s";

    private static final Map<String, Supplier<CounterBenchmark>> BENCHMARKS_MODES=new HashMap<>();

    static {
        try {
            METHODS[START]      = CounterPerf.class.getMethod("startTest");
            METHODS[GET_CONFIG] = CounterPerf.class.getMethod("getConfig");
            METHODS[SET]        = CounterPerf.class.getMethod("set", String.class, Object.class);
            METHODS[QUIT_ALL]   = CounterPerf.class.getMethod("quitAll");

            NUM_THREADS=Util.getField(CounterPerf.class, "num_threads", true);
            TIME=Util.getField(CounterPerf.class, "time", true);
            TIMEOUT=Util.getField(CounterPerf.class, "timeout", true);
            PRINT_INVOKERS=Util.getField(CounterPerf.class, "print_updaters", true);
            PRINT_DETAILS=Util.getField(CounterPerf.class, "print_details", true);
            RANGE=Util.getField(CounterPerf.class, "range", true);
            BENCHMARK = Util.getField(CounterPerf.class, "benchmark", true);
            PerfUtil.init();
            ClassConfigurator.addIfAbsent((short)1050, UpdateResult.class);

            BENCHMARKS_MODES.put("sync", SyncBenchmark::new);
            BENCHMARKS_MODES.put("async", AsyncCounterBenchmark::new);
        }
        catch(Exception e) {
            throw new RuntimeException(e);
        }
    }


    public void init(String props, String name, int bind_port, boolean use_fibers, String histogram_path) throws Throwable {
        histogramPath = histogram_path;
        if (histogramPath != null) {
            System.out.println("Histogram enabled! Will be stored into " + histogramPath);
        }
        thread_factory=new DefaultThreadFactory("updater", false, true)
          .useVirtualThreads(use_fibers);
        if(use_fibers && Util.virtualThreadsAvailable())
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

    public UpdateResult startTest() throws Throwable {
        System.out.printf("running for %d seconds\n", time);

        Supplier<CounterBenchmark> benchmarkSupplier = BENCHMARKS_MODES.get(benchmark.toLowerCase());
        if (benchmarkSupplier == null) {
            String msg = String.format("Benchmark %s not found!", benchmark);
            System.out.println(msg);
            throw new IllegalArgumentException(msg);
        }

        try (CounterBenchmark bm = benchmarkSupplier.get()) {
            bm.init(num_threads, thread_factory, this::getDelta, counter);

            long start = System.currentTimeMillis();
            bm.start();

            long interval = (long) ((time * 1000.0) / 10.0);
            for (int i = 1; i <= 10; i++) {
                Util.sleep(interval);
                System.out.printf("%d: %s\n", i, printAverage(start, bm));
            }

            bm.stop();
            bm.join();
            long total_time = System.currentTimeMillis() - start;

            System.out.println();
            Histogram avg_incrs = bm.getResults(print_updaters, avgMinMax -> print(avgMinMax, print_details));
            if (print_updaters)
                System.out.printf("\navg over all updaters: %s\n", print(avg_incrs, print_details));

            System.out.printf("\ndone (in %s ms)\n", total_time);

            if (histogramPath != null) {
                String fileName = String.format("histogram_%s_%s.hgrm", counter_service.raftId(), this.benchmark);
                Path filePath = Path.of(histogramPath, fileName);
                System.out.println("Storing histogram to " + filePath.toAbsolutePath());
                try {
                    HistogramUtil.writeTo(avg_incrs, filePath.toFile());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }

            return new UpdateResult(bm.getTotalUpdates(), total_time, avg_incrs);
        }
    }

    public void quitAll() {
        System.out.println("-- received quitAll(): shutting down");
        stopEventLoop();
        System.exit(0);
    }

    protected static String printAverage(long start_time, CounterBenchmark benchmark) {
        long tmp_time=System.currentTimeMillis() - start_time;
        long incrs=benchmark.getTotalUpdates();
        double incrs_sec=incrs / (tmp_time / 1000.0);
        return String.format("%,.0f updates/sec (%,d updates)", incrs_sec, incrs);
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
        boolean counter_available;
        long cnt=0;
        while(looping) {
            try {
                try {
                    cnt=getCounter();
                    counter_available=true;
                }
                catch(Throwable t) {
                    counter_available=false;
                }

                int c=Util.keyPress(String.format(format, num_threads, Util.printTime(time, TimeUnit.MILLISECONDS),
                                                  range, Util.printTime(timeout, TimeUnit.MILLISECONDS),
                                                  print_details, print_updaters, benchmark,
                                                  counter_available? String.format(" (counter=%d)\n", cnt) : "\n"));
                switch(c) {
                    case '1':
                        startBenchmark();
                        break;
                    case '2':
                        printView();
                        break;
                    case '4':
                        changeFieldAcrossCluster(NUM_THREADS, Util.readIntFromStdin("Number of updater threads: "));
                        break;
                    case '6':
                        changeFieldAcrossCluster(TIME, Util.readIntFromStdin("Time (secs): "));
                        break;
                    case 'd':
                        changeFieldAcrossCluster(PRINT_DETAILS, !print_details);
                        break;
                    case 'i':
                        changeFieldAcrossCluster(PRINT_INVOKERS, !print_updaters);
                        break;
                    case 't':
                        changeFieldAcrossCluster(TIMEOUT, Util.readIntFromStdin("update timeout (ms): "));
                        break;
                    case 'r':
                        changeFieldAcrossCluster(RANGE, Util.readIntFromStdin("range: "));
                        break;
                    case 'b':
                        changeFieldAcrossCluster(BENCHMARK, Util.readStringFromStdin("benchmark mode: "));
                        break;
                    case 'v':
                        System.out.printf("Version: %s, Java version: %s\n", Version.printVersion(),
                                          System.getProperty("java.vm.version", "n/a"));
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
    void startBenchmark() {
        RspList<UpdateResult> responses;
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
        Histogram globalHistogram=null;

        System.out.println("\n======================= Results: ===========================");
        for(Map.Entry<Address,Rsp<UpdateResult>> entry: responses.entrySet()) {
            Address mbr=entry.getKey();
            Rsp<UpdateResult> rsp=entry.getValue();
            UpdateResult result=rsp.getValue();
            if(result != null) {
                total_incrs+=result.num_updates;
                total_time+=result.total_time;
                if(globalHistogram == null)
                    globalHistogram=result.histogram;
                else
                    globalHistogram.add(result.histogram);
            }
            System.out.println(mbr + ": " + result);
        }
        double total_reqs_sec=total_incrs / ( total_time/ 1000.0);
        System.out.println("\n");
        System.out.println(Util.bold(String.format("Throughput: %,.2f updates/sec/node\n" +
                                                   "Time:       %s / update\n",
                                                   total_reqs_sec, print(globalHistogram, print_details))));
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

    protected static String print(AbstractHistogram histogram, boolean details) {
        double avg = histogram.getMean();
        return details? String.format("min/avg/max = %d/%f/%s", histogram.getMinValue(), avg, histogram.getMaxValue()) :
          String.format("%s", Util.printTime(avg, TimeUnit.NANOSECONDS));
    }

    protected long getCounter() throws Exception {
        if(counter == null)
            counter=counter_service.getOrCreateCounter(COUNTER, 0);
        return counter.sync().get();
    }

    protected int getDelta() {
        long random=Util.random(range);
        int retval=(int)(tossWeightedCoin(.5)? -random : random);
        if(retval < 0 && counter.getLocal() < 0)
            retval=-retval;
        return retval;
    }

    public static boolean tossWeightedCoin(double probability) {
        if(probability >= 1)
            return true;
        if(probability <= 0)
            return false;
        long r=Util.random(1000);
        long cutoff=(long)(probability * 1000);
        return r <= cutoff;
    }


    protected static class UpdateResult implements Streamable {
        protected long          num_updates;
        protected long          total_time;     // in ms
        protected Histogram     histogram; // in ns

        public UpdateResult() {
        }

        public UpdateResult(long num_updates, long total_time, Histogram histogram) {
            this.num_updates=num_updates;
            this.total_time=total_time;
            this.histogram=histogram;
        }

        @Override
        public void writeTo(DataOutput out) throws IOException {
            Bits.writeLongCompressed(num_updates, out);
            Bits.writeLongCompressed(total_time, out);
            Util.objectToStream(histogram, out);
        }

        @Override
        public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
            num_updates=Bits.readLongCompressed(in);
            total_time=Bits.readLongCompressed(in);
            histogram = Util.objectFromStream(in);
        }

        public String toString() {
            double total_reqs_per_sec=num_updates / (total_time / 1000.0);
            return String.format("%,.2f updates/sec (%,d updates, %s / update)", total_reqs_per_sec, num_updates,
                                 Util.printTime(histogram.getMean(), TimeUnit.NANOSECONDS));
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException {
        String  props=null, name=null, histogram_path = null;
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
            if ("-histogram".equals(args[i])) {
                histogram_path = args[++i];
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
            test.init(props, name, port, use_fibers, histogram_path);
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
                             "[-use_fibers <true|false>] [-histogram /path/to/write/log]");
    }


}