# Recover a payment after native WalletQuery disconnects

Run this case to check whether a still-running Zallet daemon can recover an incoming shielded payment after its connection to native WalletQuery disappears. The payer is Zingo CLI on the same isolated Regtest node through lightwalletd.

## Run the case

Prepare the [wallet clients, Python environment, and fixture inputs](roundtrip.md#prepare-the-clients). Select a new private output directory and an available loopback port base. The private relay listens at the base plus 90.

```bash
"$wallet_python" -B scripts/wallet-clients/transport-payment.py \
  --output "$evidence/native-disconnect" --port-base 20900 \
  --images "$fixture_images" --bin-dir "$release_bin_dir" --revision "$release_sha" \
  --zingo "$zingo" --zallet-backend "$zallet_backend"
```

Both wallets and the control node first observe the pending 0.10 TAZ transfer. The runner stops the native relay and checks that its listener is down while the native service, lightwalletd, and control node remain available. It mines ten blocks, checks that the original payment is canonical, and restores the same local endpoint.

After reconnecting, the original Zallet process must scan through the node tip recorded during the outage. The same wallet snapshot must show exactly one confirmed payment at its canonical height, with the expected amount and decrypted fixture memo. The runner also checks the fee and spendable balance. Zallet then signs a 0.04 TAZ return. A subsequent intentional wallet restart verifies that both ledgers persisted. Private logs and wallet data stay beside the receipt.

This exercises one interrupted native connection on Regtest. For a stopped wallet or a shallow rollback, use the [payment recovery cases](payment-recovery.md).
