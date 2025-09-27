package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.protocols.raft.AppendEntriesRequest;
import org.jgroups.protocols.raft.AppendEntriesResponse;
import org.jgroups.protocols.raft.AppendResult;
import org.jgroups.protocols.raft.InstallSnapshotRequest;
import org.jgroups.protocols.raft.REDIRECT;
import org.jgroups.protocols.raft.RaftHeader;
import org.jgroups.protocols.raft.election.VoteRequest;
import org.jgroups.protocols.raft.election.VoteResponse;
import org.jgroups.raft.Options;
import org.jgroups.util.ByteArrayDataOutputStream;
import org.jgroups.util.Util;

import org.testng.annotations.Test;

import static org.jgroups.protocols.raft.AppendResult.Result.FAIL_ENTRY_NOT_FOUND;

/**
 * @author Bela Ban
 * @since  0.1
 */
@Test(groups=Global.FUNCTIONAL)
public class RaftHeaderTest {

    public void testVoteRequestHeader() throws Exception {
        VoteRequest hdr=new VoteRequest(22);
        _testSize(hdr, VoteRequest.class);
    }

    public void testVoteResponseHeader() throws Exception {
        VoteResponse rsp=new VoteResponse(22, 3, 7);
        _testSize(rsp, VoteResponse.class);
    }


    public void testAppendEntriesRequest() throws Exception {
        AppendEntriesRequest req=new AppendEntriesRequest(Util.createRandomAddress("A"), 22, 4, 21, 22, 18);
        _testSize(req, AppendEntriesRequest.class);
    }

    public void testAppendEntriesResponse() throws Exception {
        AppendEntriesResponse rsp=new AppendEntriesResponse(22, new AppendResult(FAIL_ENTRY_NOT_FOUND, 22, 5));
        _testSize(rsp, AppendEntriesResponse.class);
    }

    public void testInstallSnapshotHeader() throws Exception {
        InstallSnapshotRequest hdr=new InstallSnapshotRequest(5);
        _testSize(hdr, InstallSnapshotRequest.class);

        hdr=new InstallSnapshotRequest(5, Util.createRandomAddress("A"), 5, 4);
        _testSize(hdr, InstallSnapshotRequest.class);
    }

    public void testRedirectHeader() throws Exception {
        REDIRECT.RedirectHeader hdr=new REDIRECT.RedirectHeader(REDIRECT.RequestType.REQ, 22, true);
        _testSize(hdr, REDIRECT.RedirectHeader.class);
        hdr.options(Options.create(true));

        hdr=new REDIRECT.RedirectHeader(REDIRECT.RequestType.RSP, 322649, false)
          .options(Options.create(true));
        _testSize(hdr, REDIRECT.RedirectHeader.class);
    }


    protected static <T extends RaftHeader> void _testSize(T hdr, Class<T> clazz) throws Exception {
        int size=hdr.serializedSize();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size);
        hdr.writeTo(out);
        System.out.println(clazz.getSimpleName() + ": size=" + size);
        assert out.position() == size;

        RaftHeader hdr2=Util.streamableFromByteBuffer(clazz, out.buffer(), 0, out.position());
        assert hdr2 != null;
        assert hdr.currTerm() == hdr2.currTerm();
    }


    protected static <T extends REDIRECT.RedirectHeader> void _testSize(T hdr, Class<T> clazz) throws Exception {
        int size=hdr.serializedSize();
        ByteArrayDataOutputStream out=new ByteArrayDataOutputStream(size);
        hdr.writeTo(out);
        System.out.println(clazz.getSimpleName() + ": size=" + size);
        assert out.position() == size;

        REDIRECT.RedirectHeader hdr2=Util.streamableFromByteBuffer(clazz, out.buffer(), 0, out.position());
        assert hdr2 != null;
        assert hdr.serializedSize() == hdr2.serializedSize();
    }
}
