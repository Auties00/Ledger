/// Ledger, an embedded storage engine for applications with one writing process that need their data durable,
/// encrypted and ordered.
///
/// The API is the package `com.github.auties00.ledger`; start from `Ledger.builder()`.
///
/// The module `jdk.management` is optional: it tells which collector the JVM runs, and without it a store never packs
/// the values of its fastest commits into arrays that collector would not copy.
module com.github.auties00.ledger {
    // ExtendedOpenOption.DIRECT, which opens the log's segments with O_DIRECT to write groups through on Linux
    requires jdk.unsupported;
    requires static jdk.management;

    exports com.github.auties00.ledger;
}
