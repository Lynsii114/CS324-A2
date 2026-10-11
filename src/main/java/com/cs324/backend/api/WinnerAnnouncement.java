package com.cs324.backend.api;

import java.io.Serializable;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * Result of an election, broadcast to every participant so each worker records
 * the same coordinator.
 */
public final class WinnerAnnouncement implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String electionId;
    private final CandidateInfo winner;
    private final Set<CandidateInfo> participants;

    public WinnerAnnouncement(String electionId, CandidateInfo winner, Set<CandidateInfo> participants) {
        this.electionId = electionId;
        this.winner = winner;
        this.participants = new LinkedHashSet<>(participants);
    }

    public String getElectionId() {
        return electionId;
    }

    public CandidateInfo getWinner() {
        return winner;
    }

    public Set<CandidateInfo> getParticipants() {
        return participants;
    }

    @Override
    public String toString() {
        return "WinnerAnnouncement{electionId='" + electionId + "', winner=" + winner
                + ", participants=" + participants.size() + "}";
    }
}