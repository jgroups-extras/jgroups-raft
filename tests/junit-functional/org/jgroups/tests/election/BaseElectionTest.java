package org.jgroups.tests.election;

import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.ELECTION2;
import org.jgroups.protocols.raft.election.BaseElection;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;

public class BaseElectionTest {
    public static final String ALL_ELECTION_CLASSES_PROVIDER = "all-election-classes";

    protected Class<? extends BaseElection> electionClass;

    public BaseElection instantiate() throws Exception {
        assert electionClass != null : "Election class not set";
        // The default constructor is always available.
        return electionClass.getDeclaredConstructor().newInstance();
    }

    @BeforeMethod(alwaysRun = true)
    @SuppressWarnings("unchecked")
    protected void setElectionClass(Object[] args) {
        electionClass = (Class<? extends BaseElection>) args[0];
    }

    @DataProvider(name = ALL_ELECTION_CLASSES_PROVIDER)
    protected static Object[][] electionClasses() {
        return new Object[][] {
                {ELECTION.class},
                {ELECTION2.class},
        };
    }
}
