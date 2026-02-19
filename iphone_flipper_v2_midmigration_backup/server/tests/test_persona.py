from __future__ import annotations

import random
import unittest

from server.services.worker.persona import generate_persona, persona_hash


class PersonaTests(unittest.TestCase):
    def test_generate_persona_matches_geo_country(self) -> None:
        persona = generate_persona(proxy_geo={"country_code": "AU"}, rng=random.Random(7))
        self.assertTrue(persona.timezone_id.startswith("Australia/"))
        self.assertEqual(persona.locale, "en-AU")

    def test_generate_persona_uses_fallback_when_geo_unknown(self) -> None:
        persona = generate_persona(proxy_geo={"country_code": "ZZ"}, rng=random.Random(11))
        self.assertIn(persona.timezone_id, {"America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles"})
        self.assertEqual(persona.locale, "en-US")

    def test_persona_hash_is_deterministic(self) -> None:
        rng = random.Random(19)
        persona_one = generate_persona(proxy_geo={"country_code": "US"}, rng=rng)
        rng = random.Random(19)
        persona_two = generate_persona(proxy_geo={"country_code": "US"}, rng=rng)
        self.assertEqual(persona_hash(persona_one), persona_hash(persona_two))

    def test_persona_generation_varies_across_cycles(self) -> None:
        hashes = {
            persona_hash(generate_persona(proxy_geo={"country_code": "US"}, rng=random.Random(seed)))
            for seed in range(1, 10)
        }
        self.assertGreater(len(hashes), 1)


if __name__ == "__main__":
    unittest.main()
