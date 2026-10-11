package com.cs324.backend.api;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * An ELECTION message that propagates hop-by-hop through neighbouring workers.
 * Carries a unique electionId (for deduplication) and the growing set of
 * reachable participants each hop has discovered so far.
 */
public final class ElectionMessage implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String electionId;
    private final CandidateInfo originator;
    private final CandidateInfo sender;
    private final Set<CandidateInfo> participants;

    public ElectionMessage(String electionId, CandidateInfo originator, CandidateInfo sender,
                           Set<CandidateInfo> participants) {
        this.electionId = electionId;
        this.originator = originator;
        this.sender = sender;
        this.participants = new LinkedHashSet<>(participants);
    }

    public String getElectionId() {
        return electionId;
    }

    public CandidateInfo getOriginator() {
        return originator;
    }

    public CandidateInfo getSender() {
        return sender;
    }

    public Set<CandidateInfo> getParticipants() {
        return participants;
    }

    @Override
    public String toString() {
        return "ElectionMessage{electionId='" + electionId + "', sender=" + sender
                + ", participants=" + participants.size() + "}";
    }
}