package org.jgroups.raft.tests.harness;

import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.InMemoryLog;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.election.BaseElection;
import org.jgroups.stack.Protocol;
import org.jgroups.raft.tests.DummyStateMachine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Supplier;

import org.testng.ITestContext;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

/**
 * The base class for the Raft tests.
 * <p>
 * This class is abstract and does not enforce which type of cluster/communication is utilized between the nodes. It
 * requires the implementation of the creation, destruction, and communication pieces instead. Therefore, nodes can
 * utilize {@link org.jgroups.JChannel}, or {@link org.jgroups.raft.testfwk.MockRaftCluster} without implementing
 * everything again. Some of the utilities provided:
 *
 * <ul>
 *     <li>Entry point during the lifecycle to customize configurations.</li>
 *     <li>A default configuration to utilize and share between the tests.</li>
 *     <li>Utilities functions to access the channels and protocols.</li>
 * </ul>
 *
 * With a central base class, it is easier to apply changes instead of creating a boiler plate code copied through
 * many test classes. There is also subclasses specialized for different scenarios.
 * </p>
 * <p>
 * When utilizing the base class with a custom data provider factory, to receive the arguments properly, the
 * {@link #passDataProviderParameters(Object[])} needs to be overridden.
 * </p>
 *
 * <p></p>
 *
 * <h2>Configuration</h2>
 * <p>
 * The basic configuration in the class has:
 * <ul>
 *     <li>{@link #clusterSize}: The number of nodes in the test.</li>
 *     <li>{@link #recreatePerMethod}: If the cluster is created and strip-down per method execution.</li>
 *     <li>{@link #createManually}: If the lifecycle management of creating and destroying is manual.</li>
 * </ul>
 * </p>
 *
 * <h3>Trace logging</h3>
 * <p>
 * There is also a configuration by command-line arguments to enable tracing during the tests. To enable trace level
 * globally, to all protocols in the stack, pass the variable {@link #ENABLE_GLOBAL_TRACE}, that is:
 *
 * <pre>
 *     $ mvn clean test -Dorg.jgroups.test.trace
 * </pre>
 *
 * We also provide an alias with
 *
 * <pre>
 *     $ mvn clean test -Dtrace
 * </pre>
 *
 * Any of the above will enable trace to all protocols. The output is not directed to the standard output, it can be
 * found at the generated reports. To instead enable trace for specific classes, is necessary to use the complete
 * variable {@link #ENABLE_TRACE_CLASSES}, passing the classes fully qualified name:
 *
 * <pre>
 *     $ mvn clean test -Dorg.jgroups.test.trace="org.jgroups.protocols.raft.RAFT"
 * </pre>
 *
 * This will enable trace level only for the {@link RAFT} class. It is possible to pass multiple classes separated
 * by comma.
 * </p>
 *
 * @since 1.0.13
 */
public abstract class AbstractRaftTest {

    /**
     * Enable trace log level to all classes in the protocol stack.
     */
    public static final String ENABLE_GLOBAL_TRACE = "org.jgrops.test.trace";

    /**
     * Enable trace log level to the specific list of classes given in FQN form and comma separated.
     */
    public static final String ENABLE_TRACE_CLASSES = "org.jgroups.test.traces";

    // Configurable parameters for sub-classes.

    /**
     * Defines the size of the cluster, i.e., the number of nodes.
     */
    protected int clusterSize = 1;

    /**
     * Defines if the cluster should be cleared and re-created on a per-method basis.
     */
    protected boolean recreatePerMethod = false;

    /**
     * Defines if the cluster creation should happen manually. Therefore, both creation and clear happens manually.
     */
    protected boolean createManually = false;

    /**
     * Test executor to run operations asynchronously.
     */
    protected final ExecutorService executor = Executors.newCachedThreadPool(t -> new Thread(t, "testing"));

    @AfterClass
    protected final void cleanup() {
        executor.shutdown();
    }

    /**
     * {@link #clusterSize}.
     *
     * @param size: The cluster size to create.
     * @return This test class.
     */
    protected final <T extends AbstractRaftTest> T withClusterSize(int size) {
        this.clusterSize = size;
        return self();
    }

    /**
     * {@link #recreatePerMethod}.
     *
     * @param value: The value to use.
     * @return This test class.
     */
    protected final <T extends AbstractRaftTest> T withRecreatePerMethod(boolean value) {
        this.recreatePerMethod = value;
        return self();
    }

    /**
     * {@link #createManually}.
     *
     * @param value: The value to use.
     * @return This test class.
     */
    protected final <T extends AbstractRaftTest> T withManuallyCreate(boolean value) {
        this.createManually = value;
        return self();
    }

    @BeforeClass(alwaysRun = true)
    protected final void initialize(ITestContext ctx) throws Exception {
        if (!createManually && !recreatePerMethod) createCluster();
    }

    @BeforeMethod(alwaysRun = true)
    protected final void initializeMethod(ITestContext ctx, Object[] args) throws Exception {
        passDataProviderParameters(args);
        if (!createManually && recreatePerMethod) createCluster();
    }

    @AfterClass(alwaysRun = true)
    protected final void teardown() throws Exception {
        if (!createManually && !recreatePerMethod) destroyCluster();
    }

    @AfterMethod(alwaysRun = true)
    protected final void teardownMethod() throws Exception {
        if (!createManually && recreatePerMethod) destroyCluster();
    }

    @SuppressWarnings("unchecked")
    protected final <T extends AbstractRaftTest> T self() {
        return (T) this;
    }

    /**
     * Creates the cluster using the provided configuration of size but limiting the number of nodes to create.
     * <p>
     * This method limits the number of nodes created at once. Calling the method multiple times is safe, and the
     * maximum {@link #clusterSize} is never violated.
     * </p>
     *
     * @param limit: The number of nodes to create at once. Never creates more than {@link #clusterSize} nodes.
     * @throws Exception: If an error occur while creating the cluster.
     */
    protected abstract void createCluster(int limit) throws Exception;

    /**
     * Destroy the cluster and clears any open resource.
     * <p>
     * The implementors must invoke the appropriate {@link #beforeClusterDestroy()} and {@link #afterClusterDestroy()}
     * methods to notify listeners.
     * </p>
     *
     * @throws Exception: If failed clearing the resources.
     */

    /**
     * Destroy the cluster.
     * <p>
     * This method clears all resources allocated to the cluster. The cluster should be recreated again for use.
     * </p>
     *
     * @throws Exception: If an error happens while clearing the resources.
     */
    protected abstract void destroyCluster() throws Exception;

    /**
     * Creates the protocol stack to utilize during the tests.
     *
     * @param name: The name of node requesting the stack.
     * @return The complete protocol stack.
     * @throws Exception: If an error occur while creating the stack.
     */
    protected abstract Protocol[] baseProtocolStackForNode(String name) throws Exception;

    /**
     * Apply the log level configuration.
     * <p>
     * Check {@link AbstractRaftTest} about how to configure the log level. Note that, a log instance might be static,
     * and since the tests run in parallel, this configuration might leak between tests!
     * </p>
     *
     * @param stack: The protocol stack in use.
     */
    protected final void applyTraceConfiguration(Protocol[] stack) {
        String[] traces = Arrays.stream(System.getProperty(ENABLE_TRACE_CLASSES, "").split(","))
                .filter(s -> !s.isEmpty())
                .toArray(String[]::new);
        boolean globalTrace = Boolean.parseBoolean(System.getProperty(ENABLE_GLOBAL_TRACE, "false"));

        if (globalTrace) System.out.println("Enabling trace on all classes");
        else if (traces.length > 0) System.out.printf("Enabling trace on classes: %s%n", Arrays.toString(traces));

        Set<String> classes = Set.of(traces);
        for (Protocol protocol : stack) {
            if (globalTrace || classes.contains(protocol.getClass().getName())) {
                protocol.level("trace");
            }
        }
    }

    /**
     * Creates the cluster using the provided configuration of size.
     * <p>
     * This method can be invoked multiple times without harm in the tests.
     * </p>
     *
     * <p>
     * The lifecycle methods are invoked when appropriately. The cluster lifecycle methods are always invoked.
     * </p>
     *
     * @throws Exception: If an error occur while creating the cluster.
     */
    protected final void createCluster() throws Exception {
        beforeClusterCreation();
        createCluster(clusterSize);
        afterClusterCreation();
    }

    /**
     * The cluster name to connect the channels. Defaults to the test class name.
     *
     * @return The cluster name.
     */
    protected String clusterName() {
        return getClass().getSimpleName();
    }

    /**
     * Creates a new {@link RAFT} instance.
     * <p>
     * The default configuration has configured:
     *
     * <ul>
     *     <li>The raft ID.</li>
     *     <li>The members IDs, based on the offset from <code>'A'</code> and the {@link #clusterSize}</li>.
     *     <li>The log to use with {@link #getRaftLogClass()}.</li>
     *     <li>The log prefix, the raft ID concat with the cluster name.</li>
     * </ul>
     * This method is final, to update any configuration, override the {@link #amendRAFTConfiguration(RAFT)}. The
     * method is invoked before returning.
     * </p>
     *
     * @param raftId: The ID of the node.
     * @return A new and properly configured {@link RAFT} instance.
     */
    protected final RAFT createNewRaft(String raftId) {
        RAFT r = newRaftInstance()
                .raftId(raftId)
                .members(getRaftMembers())
                .logClass(getRaftLogClass())
                .stateMachine(new DummyStateMachine())
                .logPrefix(String.format("%s-%s", raftId, clusterName()));
        amendRAFTConfiguration(r);
        return r;
    }

    /**
     * Creates a new instance of the {@link RAFT} protocol.
     * <p>
     * This method exist so subclasses can override with custom implementations.
     * </p>
     *
     * @return A new {@link RAFT} instance.
     */
    protected RAFT newRaftInstance() {
        return new RAFT();
    }

    /**
     * Creates the election algorithm and invokes the entry point to decorate.
     * <p>
     * This method is final, to define which election algorithm to use, override {@link #createNewElection()}.
     * </p>
     *
     * @return A new and properly configured {@link BaseElection} concrete implementation.
     * @throws Exception: If failed creating the election instance.
     */
    protected final BaseElection createNewElectionAndDecorate() throws Exception {
        BaseElection be = createNewElection();
        amendBaseElectionConfiguration(be);
        return be;
    }

    /**
     * Creates the election algorithm to use.
     * <p>
     * The default election implementation is {@link ELECTION}. This method is not final and could be override to
     * create a different instance. There is also the {@link #amendBaseElectionConfiguration(BaseElection)} to change
     * the protocol configuration.
     * </p>
     *
     * @return A new and properly configured {@link BaseElection} concrete implementation.
     */
    protected BaseElection createNewElection() throws Exception {
        return new ELECTION();
    }

    /**
     * Retrieves the members utilized in the {@link RAFT} configuration.
     * <p>
     * This implementation offset from <code>'A'</code> up to {@link #clusterSize} members. That is, for a size of
     * <code>2</code>, creates a list with <code>'A', 'B'</code>.
     * </p>
     *
     * @return The list of members in the cluster.
     */
    protected final Collection<String> getRaftMembers() {
        List<String> members = new ArrayList<>(clusterSize);
        for (int i = 0; i < clusterSize; i++) {
            members.add(Character.toString('A' + i));
        }

        return members;
    }

    /**
     * Execute the supplier asynchronously on the test executor.
     *
     * @param supplier: The operation to execute asynchronously.
     * @return The {@link CompletableFuture} identifying the supplier asynchronous execution.
     * @param <T>: The return type of the given supplier.
     */
    protected final <T> CompletableFuture<T> async(Supplier<T> supplier) {
        return CompletableFuture.supplyAsync(supplier, executor);
    }

    /**
     * Execute the given runnable asynchronously on the test executor.
     *
     * @param runnable: The operation to execute.
     * @return The {@link CompletableFuture} identifying the runnable asynchronous execution.
     */
    protected final CompletableFuture<Void> async(Runnable runnable) {
        return CompletableFuture.runAsync(runnable, executor);
    }

    /**
     * The {@link org.jgroups.protocols.raft.Log} to use in {@link RAFT}.
     * Defaults to {@link InMemoryLog}.
     *
     * @return The log class to use.
     */
    protected String getRaftLogClass() {
        return InMemoryLog.class.getName();
    }

    /**
     * Entrypoint to change configurations of the {@link RAFT} instance before start.
     * <p>
     * This method is invoked every time a new {@link RAFT} instance is created.
     * </p>
     *
     * @param raft: The {@link RAFT} instance with basic configuration.
     * @see #createNewRaft(String)
     */
    protected void amendRAFTConfiguration(RAFT raft) { }

    /**
     * Entrypoint to change configuration of the {@link BaseElection} instance before start.
     * <p>
     * This method is invoked every time a new {@link BaseElection} instance is created, the actual concrete type
     * might vary depending on the test.
     * </p>
     *
     * @param election: The instance with base configuration.
     * @see #createNewElection()
     */
    protected void amendBaseElectionConfiguration(BaseElection election) { }

    /**
     * Necessary when utilizing the base class with a data provider factory.
     *
     * @param args: The data provider arguments.
     */
    protected void passDataProviderParameters(Object[] args) { }

    /**
     * Entrypoint executed before the cluster is created.
     * This method might be invoked multiple times during a single test.
     *
     * @throws Exception: If an exception happens during the execution.
     */
    protected void beforeClusterCreation() throws Exception { }

    /**
     * Entrypoint executed before destroying the cluster.
     *
     * @throws Exception: If an exception happens during the execution.
     */
    protected void beforeClusterDestroy() throws Exception { }

    /**
     * Entrypoint executed after the cluster is created and running.
     * This method can execute multiple times during a single test, and executes after all channels have the same view.
     *
     * @throws Exception: If an exception happens during execution.
     * @see #beforeClusterCreation()
     */
    protected void afterClusterCreation() throws Exception { }

    /**
     * Entrypoint executed after the cluster is destroyed.
     *
     * @throws Exception: If an exception happens during execution.
     */
    protected void afterClusterDestroy() throws Exception { }
}
