package org.jgroups.protocols.raft;

import org.jgroups.Address;

import java.util.Map;

/**
 * @author Bela Ban
 * @since x.y
 */
public class DummyLog implements Log {

    public DummyLog() {
    }

    public void init(Map<String,String> args) {
        System.out.println("args = " + args);
    }

    public void destroy() {

    }

    public int currentTerm() {
        return 0;
    }

    public Log currentTerm(int new_term) {
        return null;
    }

    public Address votedFor() {
        return null;
    }

    public Log votedFor(Address member) {
        return null;
    }

    public int first() {
        return 0;
    }

    public int commitIndex() {
        return 0;
    }

    public Log commitIndex(int new_index) {
        return null;
    }

    public int lastApplied() {
        return 0;
    }

    public AppendResult append(int prev_index,int prev_term,LogEntry[] entries) {
        return null;
    }

    public void forEach(Function function,int start_index,int end_index) {

    }

    public void forEach(Function function) {

    }
}
