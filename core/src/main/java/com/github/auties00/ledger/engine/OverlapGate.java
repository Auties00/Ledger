package com.github.auties00.ledger.engine;

import java.util.function.LongSupplier;

/// Decides, epoch by epoch, whether the log writes its groups beside one another, for [LogWriter.Overlap#AUTO].
///
/// An epoch is at least [#EPOCH_GROUPS] published groups and [#EPOCH_NANOS]; its rate is the commits it made durable a
/// nanosecond. The rate of a closed loop of committers is what a second group on the device changes: a device that
/// serves two writes no faster than one, or groups split too small to pay for their writes, show in it, where the time
/// of a write alone does not. An epoch on one lane most of whose groups were written through and woke two or more
/// commits is followed by a try on two lanes. A try whose rate beats the epoch before it by a sixteenth keeps groups
/// beside one another for [#KEEP] more epochs, after which one epoch on one lane measures that rate again, and a try
/// again follows; one that does not gives way to 2, then 4, up to 64 epochs on one lane before the next. An epoch
/// longer than [#EPOCH_MAX_NANOS] measures an idle store rather than a rate, and decides nothing.
///
/// Not thread-safe: the log calls it holding its lane lock.
final class OverlapGate {
    /// The groups an epoch takes at least: a rate over fewer moves with a single slow write.
    static final int EPOCH_GROUPS = 128;
    /// The nanoseconds an epoch takes at least.
    static final long EPOCH_NANOS = 10_000_000;
    /// The nanoseconds past which an epoch measures an idle store, and decides nothing.
    static final long EPOCH_MAX_NANOS = 500_000_000;
    /// The epochs beside one another that follow a try that won, before one lane is measured again.
    static final int KEEP = 16;
    /// The tries in a row that lost past which the epochs on one lane between two tries grow no longer: after the n-th,
    /// 2 to the power of n of them.
    static final int MAX_MISSES = 6;
    /// The epochs on one lane a store measures first.
    static final int FIRST_SOLO = 2;

    private final LongSupplier clock;
    // The epoch: when it began, the sequence durable then, its groups, those written through, and those that woke two
    // or more commits
    private long epochStart;
    private long epochSequence;
    private int groups;
    private int through;
    private int shared;
    // The rate of the last epoch on one lane, in commits a nanosecond
    private double soloRate;
    // Whether the epoch beside one another is a try, which its rate decides at its end
    private boolean probing;
    // The tries in a row that lost, the epochs on one lane before the next try, and those beside one another before one
    // on one lane
    private int misses;
    private int soloLeft;
    private int keepLeft;
    // Whether groups go beside one another now
    private boolean on;

    /// Starts the first epoch, on one lane, at the sequence `sequence`, reading the time from `clock` in nanoseconds.
    OverlapGate(LongSupplier clock, long sequence) {
        this.clock = clock;
        this.epochStart = clock.getAsLong();
        this.epochSequence = sequence;
        this.soloLeft = FIRST_SOLO;
    }

    /// Counts a published group and, at the end of an epoch, decides the next one.
    ///
    /// @param sequence the commit up to which the group made the log durable
    /// @param written whether the group was written through
    /// @param woken the commits its publication woke
    /// @return whether groups go beside one another from now on
    boolean published(long sequence, boolean written, int woken) {
        groups++;
        if (written) {
            through++;
        }
        if (woken >= 2) {
            shared++;
        }
        if (groups < EPOCH_GROUPS) {
            return on;
        }
        var now = clock.getAsLong();
        var elapsed = now - epochStart;
        if (elapsed < EPOCH_NANOS) {
            return on;
        }
        var rate = (double) (sequence - epochSequence) / elapsed;
        var busy = through * 2 >= groups && shared * 2 >= groups;
        epochStart = now;
        epochSequence = sequence;
        groups = 0;
        through = 0;
        shared = 0;
        if (elapsed > EPOCH_MAX_NANOS) {
            if (probing) {
                // The try measured nothing: it runs again after the next epoch on one lane
                probing = false;
                on = false;
                soloLeft = 0;
            }
            return on;
        }
        if (!on) {
            soloRate = rate;
            if (soloLeft > 0) {
                soloLeft--;
            } else if (busy) {
                probing = true;
                on = true;
            }
            return on;
        }
        if (probing) {
            probing = false;
            if (rate > soloRate * (1 + 1.0 / 16)) {
                misses = 0;
                keepLeft = KEEP;
            } else {
                misses = Math.min(misses + 1, MAX_MISSES);
                soloLeft = 1 << misses;
                on = false;
            }
        } else if (--keepLeft <= 0 || !busy) {
            soloLeft = 0;
            on = false;
        }
        return on;
    }

    /// {@return whether groups go beside one another now}
    boolean on() {
        return on;
    }
}
