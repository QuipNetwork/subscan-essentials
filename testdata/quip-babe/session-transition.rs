// Native runtime-hook regression; controlled parent state, not a network capture.
use codec::Encode;
use frame_support::traits::{FindAuthor, Hooks};
use quip_protocol_runtime::{Babe, Runtime, RuntimeGenesisConfig, Session, System};
use sp_consensus_babe::{
    digests::{PreDigest, SecondaryPlainPreDigest},
    BABE_ENGINE_ID,
};
use sp_runtime::{traits::OpaqueKeys, BuildStorage, Digest, DigestItem};

fn merge(target: &mut serde_json::Value, patch: serde_json::Value) {
    match (target, patch) {
        (serde_json::Value::Object(t), serde_json::Value::Object(p)) => {
            for (k, v) in p {
                merge(t.entry(k).or_insert(serde_json::Value::Null), v);
            }
        }
        (t, p) => *t = p,
    }
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|v| format!("{v:02x}")).collect()
}
fn main() {
    let mut config = serde_json::to_value(RuntimeGenesisConfig::default()).unwrap();
    merge(
        &mut config,
        quip_protocol_runtime::genesis_config_presets::local_config_genesis(),
    );
    let config: RuntimeGenesisConfig = serde_json::from_value(config).unwrap();
    let mut ext = sp_io::TestExternalities::new(config.build_storage().unwrap());
    ext.execute_with(|| {
        let pre = PreDigest::SecondaryPlain(SecondaryPlainPreDigest { authority_index: 0, slot: 1.into() });
        let digest = Digest { logs: vec![DigestItem::PreRuntime(BABE_ENGINE_ID, pre.encode())] };
        System::initialize(&1, &Default::default(), &digest);
        <Babe as Hooks<u32>>::on_initialize(1);
        <Session as Hooks<u32>>::on_initialize(1);
        assert_eq!(Session::current_index(), 0);
        let parent = Session::validators();
        assert_eq!(parent.len(), 2);
        let mut keys = Session::queued_keys();
        keys.reverse();
        let expected: Vec<_> = keys.iter().map(|(account, _)| account.clone()).collect();
        // Prepare a consistent parent: queued session keys and BABE
        // next authorities have the same changed order. Do not rotate manually.
        pallet_session::QueuedKeys::<Runtime>::put(&keys);
        pallet_session::QueuedChanged::<Runtime>::put(true);
        let next: Vec<_> = keys.iter().map(|(_, key)| (key.get::<sp_consensus_babe::AuthorityId>(sp_consensus_babe::KEY_TYPE).unwrap(), 1u64)).collect();
        pallet_babe::NextAuthorities::<Runtime>::put(
            frame_support::WeakBoundedVec::<_, <Runtime as pallet_babe::Config>::MaxAuthorities>::force_from(next.clone(), None)
        );
        assert_ne!(parent, expected);
        assert_eq!(Babe::next_epoch().authorities, next);
        assert_ne!(Babe::current_epoch().authorities, next);
        <Babe as Hooks<u32>>::on_finalize(1);
        let slot = 1 + quip_protocol_runtime::configs::EpochDuration::get();
        let pre = PreDigest::SecondaryPlain(SecondaryPlainPreDigest { authority_index: 0, slot: slot.into() });
        let raw = pre.encode();
        let digest = Digest { logs: vec![DigestItem::PreRuntime(BABE_ENGINE_ID, raw.clone())] };
        System::initialize(&2, &Default::default(), &digest);
        <Babe as Hooks<u32>>::on_initialize(2);
        <Session as Hooks<u32>>::on_initialize(2);
        assert_eq!(Session::current_index(), 1);
        assert_eq!(Session::validators(), expected);
        assert_eq!(Babe::current_epoch().authorities, next);
        type Author = pallet_session::FindAccountFromAuthorIndex<Runtime, Babe>;
        let author = Author::find_author(std::iter::once((BABE_ENGINE_ID, raw.as_slice()))).unwrap();
        assert_eq!(author, expected[0]);
        assert_ne!(author, parent[0]);
        let result = serde_json::json!({
            "runtimeCommit": "52a9070944d4cd2319a5cddcd2da1d8eb0a63abc",
            "sdkCommit": "845eb9986c7131056b077d14ad7249f9d65ca111",
            "kind": "controlled native runtime hooks; not a network block",
            "parentSessionIndex": 0, "postSessionIndex": 1, "slot": slot,
            "parentValidators": parent.iter().map(|v| hex(v.as_ref())).collect::<Vec<_>>(),
            "postValidators": expected.iter().map(|v| hex(v.as_ref())).collect::<Vec<_>>(),
            "digest": hex(&raw), "expectedAuthor": hex(author.as_ref())
        });
        std::fs::write(std::env::args().nth(1).expect("output JSON path"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
        println!("Session boundary: runtime hooks rotated index 0 -> 1 and author maps to post-state ordering");
    });
}
