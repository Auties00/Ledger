/// A file system for testing what a store survives: [com.github.auties00.ledger.testkit.CrashFileSystem] keeps in
/// memory what a force made durable apart from what was written since, and makes the state a loss of power or a crash
/// of the process could leave.
module com.github.auties00.ledger.testkit {
    exports com.github.auties00.ledger.testkit;
}
