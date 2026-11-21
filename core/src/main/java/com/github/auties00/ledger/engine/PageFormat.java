package com.github.auties00.ledger.engine;

/// The layout of the data file, `ledger.data`, which holds the state of a store as of its last checkpoint.
///
/// The file is a sequence of 4 KiB pages. Pages 0 and 1 hold the two superblocks, written alternately; the other
/// pages hold units a checkpoint writes into pages no durable state uses: one page for a node of a key space's tree or
/// for the delta records of several leaves ([DeltaLeaves]), a run of consecutive pages for a blob (a value too large
/// for a leaf, the catalog, the free list, the table of delta pages). Every unit is sealed on its own:
///
/// ```text
///  0  kind, 1 byte
///  1  format version, 1 byte
///  2  level of a node (0 for a leaf), 1 byte
///  3  reserved, 1 byte
///  4  pages in the unit, 4 bytes
///  8  number of its first page, 4 bytes
/// 12  bytes of content, 4 bytes
/// 16  nonce, 12 bytes
/// 28  search index of a node of the second format, 1 byte; 0 in every other unit
/// 29  reserved, 3 bytes
/// 32  content, encrypted, then zero up to the tag
///     AES-GCM tag, 16 bytes; without encryption a CRC-32C of everything before it, then 12 zero bytes
/// ```
///
/// Every unit of the data file is of format [#VERSION], but a leaf or a branch of format [#INDEXED], which holds a
/// search index between the offsets of its entries and the entries ([NodeIndex]). A reader finds the entries of either
/// through their offsets, so the first format's readers read both, and a store holds nodes of both formats.
///
/// A unit of the file of large values ([BlobFile]) in a store that encrypts whose content fits in one record,
/// [#FIRST_RECORD_CONTENT] bytes, is of format [#CLOSE_TAG], whose tag follows its content, so a reader opens the
/// content alone, straight into an array of its length:
///
/// ```text
///  0  header, as above, of format 3
/// 32  content, encrypted
///     AES-GCM tag, 16 bytes, over the header and the content
///     zeros up to the end of the unit, which no tag covers and no reader reads
/// ```
///
/// A longer one is of format [#RECORDS]: its content is sealed as records of [#RECORD] bytes, each its piece of the
/// content and a tag of its own, so a reader opens it a record at a time, each straight into its place in an array of
/// the content's length, and never holds the whole sealed unit:
///
/// ```text
///       0  header, as above, of format 4
///      32  record 0: the first FIRST_RECORD_CONTENT bytes of content, encrypted, then its AES-GCM tag, 16 bytes
///  524288  record 1: the next RECORD_CONTENT bytes of content, or the rest, encrypted, then its tag
/// i * 512 KiB  record i, likewise; the last one holds the rest of the content
///          zeros up to the end of the unit, which no tag covers and no reader reads
/// ```
///
/// Every record but the last is whole, and each after the first starts on a page, at `i * RECORD`. Record `i` is
/// sealed under the unit's nonce with `i` in the top 16 bits of its first 8 bytes, record 0 under the unit's nonce
/// itself, and its additional data is the unit's header. The first 8 bytes of the nonce of every unit of the file are
/// below 2^48 ([BlobFile#write(byte\[\], int, int)] refuses to seal more units in one open), and a content of at most
/// [Integer#MAX_VALUE] bytes takes at most 4,097 records, so no two records of the file share a nonce. The header names
/// the content's length, from which a reader counts the records, so a record read in another unit, at another index,
/// or in a unit cut short of its last records fails authentication.
///
/// The format byte is in the header, the tags' additional data, so a unit read under another format fails
/// authentication. A store without encryption writes every unit of large values in the first format, and one that
/// encrypts keeps the units of the first format and of format [#CLOSE_TAG] earlier builds wrote, so the readers of the
/// file read all three. A build that reads only the earlier formats refuses the segments of a log this build wrote by
/// their version ([LogWriter#SEGMENT_VERSION]), so it never replays a commit whose unit it would take for a torn one.
///
/// The header is the additional data of the GCM tag, so a unit read at another position, or under another kind,
/// fails authentication. The nonce of each unit is new (see [Checkpointer]), and whatever refers to a unit (a parent
/// node, a leaf holding a large value, a superblock) records its nonce: a unit left over from an earlier checkpoint
/// in the same pages fails the check even when it authenticates.
///
/// In memory, the array of a unit holds its header and content, and may end there or anywhere up to the end of the
/// unit: no reader looks past the content, so a unit a checkpoint builds holds nothing more, and one the page cache
/// reads holds what [#cachedLength(int, int)] keeps.
final class PageFormat {
    static final int SIZE = 4_096;
    static final int HEADER = 32;
    static final int TAG = 16;
    static final byte VERSION = 1;
    /// The format of a leaf or a branch that holds a search index ([NodeIndex]).
    static final byte INDEXED = 2;
    /// The format of a unit of the file of large values in a store that encrypts whose tag follows its content: the
    /// content is sealed alone, and the bytes after the tag, up to the end of the unit, are zero and not authenticated.
    static final byte CLOSE_TAG = 3;
    /// The format of a unit of the file of large values in a store that encrypts whose content is sealed as records of
    /// [#RECORD] bytes, each with its own tag: the bytes after the last record, up to the end of the unit, are zero and
    /// not authenticated.
    static final byte RECORDS = 4;
    /// The sealed bytes of a whole record of a unit of format [#RECORDS], its content and its tag, the first record's
    /// with the unit's header: 512 KiB, an array of [HeapPool] below its longest, which G1 gives no region of its own
    /// at regions of 2 MiB or more, and one call of a channel and one message of AES-GCM, which the cipher's intrinsic
    /// takes whole.
    static final int RECORD = 512 << 10;
    /// The bytes of content of a whole record of a unit of format [#RECORDS] after the first.
    static final int RECORD_CONTENT = RECORD - TAG;
    /// The bytes of content of the first record of a unit of format [#RECORDS], which follows the unit's header, so
    /// the header, the record and its tag take [#RECORD] bytes and every later record starts on a page.
    static final int FIRST_RECORD_CONTENT = RECORD - HEADER - TAG;

    static final byte SUPERBLOCK = 1;
    static final byte LEAF = 2;
    static final byte BRANCH = 3;
    static final byte VALUE = 4;
    static final byte CATALOG = 5;
    static final byte FREE_LIST = 6;
    /// A value of the file of large values, `ledger.blobs`.
    static final byte EXTERNAL = 7;
    /// The delta records of several leaves ([DeltaLeaves]).
    static final byte DELTA = 8;
    /// The table of the delta pages in use, with the records each holds ([DeltaTable]).
    static final byte DELTA_TABLE = 9;

    /// The first page a unit may use; pages 0 and 1 are the superblocks.
    static final int FIRST_UNIT_PAGE = 2;

    static final int KIND = 0;
    static final int FORMAT = 1;
    static final int LEVEL = 2;
    static final int PAGES = 4;
    static final int PAGE = 8;
    static final int LENGTH = 12;
    static final int NONCE = 16;
    /// The header byte that describes the search index of a node of format [#INDEXED].
    static final int INDEX = 28;
    /// The least padding a unit the page cache takes must have for the cache to keep it without the padding. Opening
    /// a unit into an array of the pool and copying its content out costs what opening it into an array of its own
    /// does, which the JVM zeroes first, so the bound only spares the round trip through the pool for a few bytes: with
    /// a bound of 512, a warm store of chats would keep 1,519 of the 3,791 units it caches whole, 364,000 bytes of
    /// padding.
    static final int TRIM_LEAST = 64;
    /// The most pages of a unit the page cache keeps without its padding.
    static final int TRIMMED_PAGES = 16;

    private PageFormat() {
    }

    /// {@return the bytes of content a unit of `pages` pages holds, in any format but [#RECORDS]}
    static int capacity(int pages) {
        return pages * SIZE - HEADER - TAG;
    }

    /// {@return whether a unit of `pages` pages holds `content` bytes of content, in any format but [#RECORDS]}
    static boolean fits(int pages, int content) {
        return content >= 0 && content <= capacity(pages);
    }

    /// {@return whether a unit of `pages` pages of format `format` holds `content` bytes of content}
    static boolean fits(byte format, int pages, int content) {
        return content >= 0 && sealedLength(format, content) <= (long) pages * SIZE;
    }

    /// {@return the bytes from the start of a unit of format `format` holding `content` bytes to the end of its last
    /// tag: its header, its content and its tags}
    static long sealedLength(byte format, long content) {
        var tags = format == RECORDS ? records(content) : 1;
        return HEADER + content + (long) tags * TAG;
    }

    /// {@return the records of a unit of format [#RECORDS] holding `content` bytes: one for at most
    /// [#FIRST_RECORD_CONTENT] bytes, and one more for each [#RECORD_CONTENT] bytes, or part of them, past those}
    static int records(long content) {
        return content <= FIRST_RECORD_CONTENT ? 1
                : 1 + Math.toIntExact(Math.ceilDiv(content - FIRST_RECORD_CONTENT, (long) RECORD_CONTENT));
    }

    /// {@return the offset in the content of the first byte of record `record` of a unit of format [#RECORDS]}
    static int recordContent(int record) {
        return record == 0 ? 0 : record * RECORD_CONTENT - HEADER;
    }

    /// {@return the bytes of content record `record` of a unit of format [#RECORDS] with `content` bytes of content
    /// holds}
    static int recordLength(int record, int content) {
        return Math.min(content - recordContent(record), record == 0 ? FIRST_RECORD_CONTENT : RECORD_CONTENT);
    }

    /// {@return the offset in a unit of format [#RECORDS] of the sealed bytes of record `record`: right after the
    /// header for the first, `record * RECORD` for the others}
    static long recordPosition(int record) {
        return record == 0 ? HEADER : (long) record * RECORD;
    }

    /// {@return the bytes the page cache keeps of a unit of `pages` pages with `content` bytes of content, which
    /// [fit][#fits(int, int)] them: its header and content if the unit has at most [#TRIMMED_PAGES] pages and that
    /// drops at least [#TRIM_LEAST] bytes, or the whole unit}
    ///
    /// The pages [#pagesFor(long)] gives pad a unit by less than a page and a tag, so one of more than 16 pages is kept
    /// whole: dropping its padding would copy all its content to save less than a sixteenth of it.
    static int cachedLength(int pages, int content) {
        var length = pages * SIZE;
        var kept = HEADER + content;
        return pages <= TRIMMED_PAGES && length - kept >= TRIM_LEAST ? kept : length;
    }

    /// {@return the pages of a unit holding `content` bytes, in any format but [#RECORDS]}
    static int pagesFor(long content) {
        return Math.toIntExact((content + HEADER + TAG + SIZE - 1) / SIZE);
    }

    /// {@return the pages of a unit of format `format` holding `content` bytes}
    static int pagesFor(byte format, long content) {
        return Math.toIntExact(Math.ceilDiv(sealedLength(format, content), (long) SIZE));
    }

    /// {@return the byte offset of page `page` in the file}
    static long offset(int page) {
        return (long) page * SIZE;
    }
}
