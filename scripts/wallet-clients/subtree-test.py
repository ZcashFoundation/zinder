#!/usr/bin/env python3
"""Regression checks for isolating the completion-hash intervention."""
from copy import deepcopy
import json
from pathlib import Path
import runpy
import unittest

import service_pb2 as pb


module = runpy.run_path(str(Path(__file__).with_name("subtree-ab.py")))
fixture = json.loads(module["FIXTURE"].read_bytes())


class SubtreeWireTests(unittest.TestCase):
    def test_only_completion_hash_changes(self):
        for expected in fixture["sapling_roots"]:
            root, height, completion = expected
            row = pb.SubtreeRoot(
                rootHash=bytes.fromhex(root),
                completingBlockHeight=height,
                completingBlockHash=bytes.fromhex(completion),
            ).SerializeToString()
            original, _ = module["subtree_wire"](row, expected, False)
            reversed_row, proof = module["subtree_wire"](row, expected, True)
            at = proof["offset"]
            self.assertEqual(original, row)
            self.assertEqual(original[:at], reversed_row[:at])
            self.assertEqual(original[at+32:], reversed_row[at+32:])
            self.assertEqual(original[at:at+32], reversed_row[at:at+32][::-1])
            with self.assertRaises(ValueError):
                module["subtree_wire"](reversed_row, expected, False)

    def test_comparison_rejects_different_call_or_ordinary_response(self):
        events = []
        for phase in ["restore", "restart"]:
            for arm in ["reference", "reversed"]:
                events.append({"phase": f"{arm}/{phase}", "method": "GetLightdInfo",
                               "rows": 1, "request_sha256": "request",
                               "emitted_sha256": "response", "source_sha256": "response"})
        self.assertTrue(module["compare_rpc_bytes"](events)["restore"]["same_invocation_counts"])
        for field in ["request_sha256", "source_sha256", "emitted_sha256", "rows"]:
            altered = deepcopy(events)
            altered[1][field] = "changed"
            with self.subTest(field=field), self.assertRaises(ValueError):
                module["compare_rpc_bytes"](altered)
        with self.assertRaises(ValueError):
            module["compare_rpc_bytes"](events[:-1])


if __name__ == "__main__":
    unittest.main()
