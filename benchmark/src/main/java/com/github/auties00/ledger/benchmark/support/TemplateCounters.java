package com.github.auties00.ledger.benchmark.support;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.runner.IterationType;

/// The shape of the store template a JVM measured ([TemplateFingerprint]), once per JVM: the first measured iteration
/// in which the workload calls [#record()] carries it and every other iteration reports 0, so the sum JMH reports
/// over a fork's iterations is the value of that fork's template.
///
/// Two sides of a compare read equal templates when their `templateBytes`, `templateFiles` and `templateShape` agree.
@State(Scope.Thread)
@AuxCounters(AuxCounters.Type.EVENTS)
public class TemplateCounters {
    /// The total size of the template's files.
    public long templateBytes;
    /// The number of the template's files.
    public long templateFiles;
    /// A 52-bit hash of the name and size of every file of the template.
    public long templateShape;

    private boolean measuring;

    /// Clears the counters at the start of an iteration.
    @Setup(Level.Iteration)
    public void clear(IterationParams params) {
        templateBytes = 0;
        templateFiles = 0;
        templateShape = 0;
        measuring = params.getType() == IterationType.MEASUREMENT;
    }

    /// Records the shape of the last template this JVM used, if this is the first measured iteration to ask.
    public void record() {
        if (!measuring) {
            return;
        }
        var shape = TemplateFingerprint.take();
        if (shape != null) {
            templateBytes = shape.bytes();
            templateFiles = shape.files();
            templateShape = shape.hash();
        }
    }
}
