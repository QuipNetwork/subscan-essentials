use codec::{Decode, Encode};
use quip_crypto_primitives::substrate::sr25519_fndsa512::{self as hybrid, Pair};
use sp_consensus_babe::digests::{PreDigest, PrimaryPreDigest, SecondaryVRFPreDigest};
use sp_core::{
    crypto::{VrfPublic, VrfSecret},
    Pair as _,
};

fn main() {
    let out = std::path::PathBuf::from(std::env::args().nth(1).expect("output directory"));
    std::fs::create_dir_all(&out).unwrap();
    // Public test seed, deterministic signing. Not an operational identity.
    let pair = Pair::from_seed(&[13u8; 32]);
    let sign_data = hybrid::babe::make_vrf_sign_data(&[5u8; 32], 7, 11);
    let signature = VrfSecret::vrf_sign(&pair, &sign_data);
    assert!(pair.public().vrf_verify(&sign_data, &signature));
    assert_eq!(signature.encode().len(), 96 + 731);
    let fixtures = [
        (
            "primary.scale",
            PreDigest::Primary(PrimaryPreDigest {
                authority_index: 1,
                slot: 7.into(),
                vrf_signature: signature.clone(),
            }),
        ),
        (
            "secondary-vrf.scale",
            PreDigest::SecondaryVRF(SecondaryVRFPreDigest {
                authority_index: 1,
                slot: 7.into(),
                vrf_signature: signature,
            }),
        ),
    ];
    for (name, digest) in fixtures {
        let raw = digest.encode();
        assert_eq!(raw.len(), 840);
        let mut input = raw.as_slice();
        let decoded = PreDigest::decode(&mut input).unwrap();
        assert!(input.is_empty());
        assert_eq!(decoded.authority_index(), 1);
        assert_eq!(*decoded.slot(), 7);
        assert!(pair
            .public()
            .vrf_verify(&sign_data, decoded.vrf_signature().unwrap()));
        std::fs::write(out.join(name), raw).unwrap();
        println!("{name}: 840 bytes, SDK roundtrip and hybrid VRF verification passed");
    }
}
