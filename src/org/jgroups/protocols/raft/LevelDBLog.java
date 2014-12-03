package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.util.Map;

/**
 * Created by ugol on 03/12/14.
 */

public class LevelDBLog implements Log {

    @Override
    public void init(Map<String, String> args) {

    }

    @Override
    public void destroy() {

    }

    @Override
    public int currentTerm() {
        return 0;
    }

    @Override
    public Log currentTerm(int new_term) {
        return null;
    }

    @Override
    public Address votedFor() {
        return null;
    }

    @Override
    public Log votedFor(Address member) {
        return null;
    }

    @Override
    public int first() {
        return 0;
    }

    @Override
    public int commitIndex() {
        return 0;
    }

    @Override
    public Log commitIndex(int new_index) {
        return null;
    }

    @Override
    public int lastApplied() {
        return 0;
    }

    @Override
    public AppendResult append(int prev_index, int prev_term, LogEntry[] entries) {
        return null;
    }

    @Override
    public void forEach(Function function, int start_index, int end_index) {

    }

    @Override
    public void forEach(Function function) {

    }
}
