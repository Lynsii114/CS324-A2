package com.cs324.backend.api;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Echo reply for an ELECTION message. A worker answers its forwarding neighbour
 * with the merged participant set it discovered in its subtree, so the
 * initiator learns about every reachable, active worker before selecting a
 * winner.
 */
public final class ProcessedParticipants implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String electionId;
    private final CandidateInfo sender;
    private final Set<CandidateInfo> participants;

    public ProcessedParticipants(String electionId, CandidateInfo sender, Set<CandidateInfo> participants) {
        this.electionId = electionId;
        this.sender = sender;
        this.participants = new LinkedHashSet<>(participants);
    }

    public String getElectionId() {
        return electionId;
    }

    public CandidateInfo getSender() {
        return sender;
    }

    public Set<CandidateInfo> getParticipants() {
        return participants;
    }

    @Override
    public String toString() {
        return "ProcessedParticipants{electionId='" + electionId + "', sender=" + sender
                + ", participants=" + participants.size() + "}";
    }
}