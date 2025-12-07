package org.jgroups.raft.api;

import org.jgroups.raft.JGroupsRaftStateMachine;
import org.jgroups.raft.StateMachine;
import org.jgroups.raft.StateMachineRead;
import org.jgroups.raft.StateMachineWrite;

import java.io.DataInput;
import java.io.DataOutput;
import java.util.HashMap;
import java.util.Map;

@JGroupsRaftStateMachine
public interface SimpleKVStateMachine extends StateMachine {

    @StateMachineRead(id = 1)
    String handleGet(String key);

    @StateMachineWrite(id = 2)
    void handlePut(String key, String value);

    class Impl implements SimpleKVStateMachine {
        private final Map<String, String> data = new HashMap<>();

        public String handleGet(String key) {
            return data.get(key);
        }

        @Override
        public void handlePut(String key, String value) {
            data.put(key, value);
        }

        @Override
        public void readContentFrom(DataInput in) throws Exception { }

        @Override
        public void writeContentTo(DataOutput out) throws Exception { }
    }
}
