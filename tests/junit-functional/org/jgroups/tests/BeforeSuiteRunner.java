package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.raft.ELECTION;
import org.jgroups.protocols.raft.RAFT;
import org.jgroups.protocols.raft.REDIRECT;
import org.testng.annotations.BeforeSuite;
import org.testng.annotations.Test;

/**
 * Code that is run before the suite. Loads a few classes which add their magic IDs to ClassConfigurator. This
 * prevents errors caused by data types received that have not yet been loaded
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL)
public class BeforeSuiteRunner {

    @BeforeSuite
    public void init() {
        for(Class<?> cl: new Class<?>[]{RAFT.class, ELECTION.class, REDIRECT.class}) {
            System.out.printf("loaded class %s\n", cl.getSimpleName());
        }
    }

}
