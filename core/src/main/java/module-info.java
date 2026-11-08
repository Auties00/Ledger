/// Ledger, an embedded storage engine for applications with one writing process that need their data durable,
/// encrypted and ordered.
///
/// The API is the package `com.github.auties00.ledger`; start from `Ledger.builder()`.
module com.github.auties00.ledger {
    // ExtendedOpenOption.DIRECT, which opens the log's segments with O_DIRECT to write groups through on Linux
    requires jdk.unsupported;

    exports com.github.auties00.ledger;
}
