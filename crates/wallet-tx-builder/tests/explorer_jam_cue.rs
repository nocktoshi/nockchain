//! Verify explorer JSON base64(note-data payloads): base64-decode → jam bytes → cue → `(list @ux)` UTF-8.
//! Strings from explorer API (memo/blob fields).

use bytes::Bytes;
use wallet_tx_builder::note_data::MemoDataPayload;

const BLOB_JAM: &[u8] = &[
    193, 221, 77, 6, 159, 7, 125, 131, 219, 131, 198, 65, 223, 224, 227, 224, 236, 224, 225, 224,
    233, 224, 237, 160, 111, 147, 77, 54, 198, 131, 186, 77, 6, 127, 7, 31, 7, 95, 11,
];

const MEMO_JAM: &[u8] = &[
    193, 157, 193, 179, 193, 169, 65, 193, 160, 116, 208, 55, 168, 28, 244, 14, 250, 6, 149, 131,
    194, 65, 229, 160, 118, 80, 176, 81, 49, 184, 60, 120, 61, 120, 59, 56, 60, 40, 24, 60, 25,
    124, 30, 20, 12, 78, 12, 158, 14, 62, 15, 62, 15, 30, 14, 78, 15, 158, 14, 62, 15, 110, 14,
    158, 14, 94, 14, 78, 14, 10, 6, 191, 6, 79, 7, 167, 7, 71, 7, 5, 131, 39, 131, 203, 131, 135,
    131, 187, 3, 33, 30, 8, 192, 64, 100, 6, 159, 7, 5, 131, 67, 131, 179, 131, 135, 131, 187,
    131, 130, 193, 233, 193, 223, 65, 193, 224, 210, 224, 229, 224, 239, 224, 240, 224, 229, 224,
    238, 160, 96, 240, 105, 112, 122, 112, 121, 240, 112, 240, 116, 112, 122, 80, 48, 248, 59,
    184, 57, 40, 24, 28, 25, 252, 29, 92, 30, 188, 29, 188, 30, 92, 47,
];

#[test]
fn cue_explorer_blob_and_memo_jams() {
    let blob = MemoDataPayload::from_blob(&Bytes::copy_from_slice(BLOB_JAM))
        .expect("blob jam should cue and parse as (list @ux)");
    let memo = MemoDataPayload::from_blob(&Bytes::copy_from_slice(MEMO_JAM))
        .expect("memo jam should cue and parse as (list @ux)");

    assert_eq!(
        std::str::from_utf8(&blob.bytes).unwrap(),
        "nns/v1/claim/nns.nock"
    );
    // UTF-8 stored as `(list @ux)` in memo-data; explorer JSON carries jam-cued bytes as base64.
    assert_eq!(
        std::str::from_utf8(&memo.bytes).unwrap(),
        "NYT 4/27/2026 Trump Is Dissatisfied With Iran\u{2019}s Plan to Reopen Strait of Hormuz"
    );
}
