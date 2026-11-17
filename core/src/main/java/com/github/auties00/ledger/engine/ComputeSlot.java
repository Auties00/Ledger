package com.github.auties00.ledger.engine;

import java.util.function.Function;
import java.util.function.Predicate;

/// What a batch knows of one of its writes that the commit checks at its turn: a computed write, a guard, or the write
/// of a companion of a computed write.
///
/// The batch records each such write as the plain write it resolved to when it was added, a put of the function's
/// result or a removal of the key, so everything the store does to plain writes before their turn (encoding, homes,
/// units of the file of large values, the opening and claiming of chains) applies to it unchanged; a guard, a companion
/// and a computed write that leaves its key unchanged, or that has not run, are recorded as removals the commit never
/// applies as such. At the batch's turn the commit compares [#readSequence] with the sequence of the last write claimed
/// on the key before its own, [#prior], which the sequencer records ([LiveLedger]): a write in between, or a range
/// removal of the key space after the read, sends a computed write to run again in the order of the commits, and fails
/// the commit of a guard, unless the value read then is the instance read before.
///
/// A slot belongs to its batch, which one commit at a time applies; the commit writes the fields below the read.
final class ComputeSlot {
    /// A computed write.
    static final byte COMPUTE = 0;
    /// A guard.
    static final byte GUARD = 1;
    /// The write of a companion of the computed write [#primary] slots before it.
    static final byte COMPANION = 2;
    /// What a computed write's run decided: nothing to write.
    static final byte NONE = 0;
    /// What a computed write's run decided: a put of its result.
    static final byte PUT = 1;
    /// What a computed write's run decided: a removal of the key.
    static final byte DELETE = 2;

    final byte kind;
    /// The function of a computed write, or `null`.
    final Function<Object, Object> function;
    /// The latest sequence when the value was read; 0 for a companion.
    final long readSequence;
    /// The value read, decoded, or `null` if the key had none.
    final Object observed;
    /// Whether the function ran when the write was added: `false` when a write of the key was under way then.
    final boolean ran;
    /// What the run when the write was added returned.
    final Object result;
    /// What that run decided, [#NONE], [#PUT] or [#DELETE].
    final byte effect;
    /// The outcome the caller holds, or `null` for a guard or a companion.
    final BatchComputation<Object> outcome;
    /// For a companion, how many writes before it its computed write is.
    final int primary;
    /// For a companion, whether a value has an entry in it.
    final Predicate<Object> member;
    /// The sequence of the last write claimed on the key before the commit's own, recorded as the commit is given its
    /// sequence; written and read by the committing threads in the order the pipeline sets.
    long prior;
    /// The value the last run received, as the commit applied the write.
    Object appliedPrevious;
    /// The value the write left, as the commit applied it.
    Object appliedValue;

    private ComputeSlot(byte kind, Function<Object, Object> function, long readSequence, Object observed, boolean ran,
                        Object result, byte effect, BatchComputation<Object> outcome, int primary, Predicate<Object> member) {
        this.kind = kind;
        this.function = function;
        this.readSequence = readSequence;
        this.observed = observed;
        this.ran = ran;
        this.result = result;
        this.effect = effect;
        this.outcome = outcome;
        this.primary = primary;
        this.member = member;
    }

    /// {@return the slot of a computed write}
    static ComputeSlot compute(Function<Object, Object> function, long readSequence, Object observed, boolean ran,
                               Object result, byte effect, BatchComputation<Object> outcome) {
        return new ComputeSlot(COMPUTE, function, readSequence, observed, ran, result, effect, outcome, 0, null);
    }

    /// {@return the slot of a guard of the value `observed`, read at `readSequence`}
    static ComputeSlot guard(long readSequence, Object observed) {
        return new ComputeSlot(GUARD, null, readSequence, observed, false, null, NONE, null, 0, null);
    }

    /// {@return the slot of the write of a companion of the computed write `primary` writes before it}
    static ComputeSlot companion(int primary, Predicate<Object> member) {
        return new ComputeSlot(COMPANION, null, 0, null, false, null, NONE, null, primary, member);
    }

    /// {@return whether the commit can apply what the write decided when it was added, or skip the guard's read}
    ///
    /// It can if no write of the key was claimed after the read: the last one claimed before the commit's, [#prior],
    /// is at or below [#readSequence]. A key whose chain had no claim, [#prior] 0, may have had a chain the store
    /// dropped since, whose writes the trees hold: the commit takes the highest claim of a chain dropped in the key
    /// space as its last write. A range removal of the key space after the read may have covered the key.
    ///
    /// @param droppedClaims the highest claim of a chain dropped in the key space
    /// @param removed the sequence of the last range removal of the key space
    boolean unchangedSinceRead(long droppedClaims, long removed) {
        return ran || kind == GUARD
                ? prior <= readSequence && (prior != 0 || droppedClaims <= readSequence) && removed <= readSequence
                : false;
    }
}
