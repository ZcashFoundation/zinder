"""Source labels for the wallet executables used in the counterparty baseline."""

REGISTERED_BUILDS = {
    "5f09ca06fbcb758e0fc4a903cee5eb5642ac50a47b4169aeafb8b51bec575814": {
        "engine": "vizor",
        "label": "Vizor v0.0.65",
        "source_commit": "d0edb5b13890dd33d9bb93b5c4e50fd9973fa4e6",
        "archive_sha256": "0641e4f5ad8a37250dde1f90bd4faf36b981ed4bf07ff9048966d94ea5667d0c",
        "driver_sha256": "d1bec32b05f27cf9294a2f69d81ad4e2290d9fcf0b558de9f7183d1bc319bd74",
        "lock_sha256": "35538464ffa69d29f7f880c2aa7455933ff78e8d72cd5b6c08d270e96f2384e4"
    },
    "781997402af9b2ee818a4ced9d3e7703c1c7ba6dcefdf4607320c5eec2489c94": {
        "engine": "devtool",
        "label": "zcash-devtool c8322f7e",
        "commit": "c8322f7e71ae46ab24801a721523ba83b27d911b",
        "lock_sha256": "f38f24b9cd7396a1e71feb0719b69aa4c26a2ac6355bbce233ff18b389a7d216"
    },
    "4a683b3b69370f0c9d6de512c5ae0b2c14142e7bf87a862c7937568c87eb41cd": {
        "engine": "zally",
        "label": "Zally 31ee6c3a",
        "commit": "31ee6c3ae09e109d189c6dcd691b192fb4c5b29e",
        "lock_sha256": "51cdd0b46d70fc771eb209f9af5c1c06b2cd0941bd196fca691fee8167089e13",
        "harness_sha256": "34ada52eecbb00f0c64d2f1d6efc868aec69d609035f8d11338004aef26e28fa"
    },
    "0eb8c60c097058f9750caaafcc27e9e3692a9b6cff6927f92e684784161ea79a": {
        "engine": "zingo",
        "label": "Zingo CLI from Zingolib 81d7997",
        "source": "81d799775f11e882896e8710e8af12d6d4d29a41",
        "version": "Zingo CLI 0.1.1"
    }
}


def matches_source(engine, build, record):
    return (build["engine"] == engine
            and all(record.get(key) == value for key, value in build.items() if key != "engine"))


def matches_build(engine, executable_sha256, record):
    build = REGISTERED_BUILDS.get(executable_sha256)
    return build is not None and matches_source(engine, build, record)


def matches_local_zally_build(record, build, builder_sha256):
    return (isinstance(build, dict)
            and record.get("clean") is True
            and any(matches_source("zally", source, record) for source in REGISTERED_BUILDS.values())
            and build == {"target": "zally-payment-harness", "builder_sha256": builder_sha256,
                          "commit": record["commit"], "lock_sha256": record["lock_sha256"],
                          "harness_sha256": record["harness_sha256"],
                          "engine_binary_sha256": record["engine_binary_sha256"]})
