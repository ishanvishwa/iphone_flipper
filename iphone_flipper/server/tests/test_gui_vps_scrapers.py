import unittest

import gui


class _FakeVar:
    def __init__(self, value: str = "") -> None:
        self.value = value

    def get(self) -> str:
        return self.value

    def set(self, value: str) -> None:
        self.value = value


class _FakeTreeview:
    def __init__(self) -> None:
        self.rows: dict[str, tuple[str, ...]] = {}
        self.selected: tuple[str, ...] = ()
        self.focused: str = ""
        self.seen: str | None = None

    def selection(self) -> tuple[str, ...]:
        return self.selected

    def selection_set(self, item_ids) -> None:
        if isinstance(item_ids, str):
            self.selected = (item_ids,)
            return
        self.selected = tuple(item_ids)

    def focus(self, item_id: str | None = None) -> str:
        if item_id is None:
            return self.focused
        self.focused = item_id
        return self.focused

    def see(self, item_id: str) -> None:
        self.seen = item_id

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows)

    def insert(self, _parent: str, _index: str, iid: str, values: tuple[str, ...]) -> None:
        self.rows[iid] = values

    def delete(self, item_id: str) -> None:
        self.rows.pop(item_id, None)


class _FakeStatusBar:
    def __init__(self) -> None:
        self.text = ""

    def config(self, *, text: str) -> None:
        self.text = text


class GuiVpsScraperFallbackTests(unittest.TestCase):
    def _build_gui(self) -> gui.iPhoneFlipperGUI:
        app = gui.iPhoneFlipperGUI.__new__(gui.iPhoneFlipperGUI)
        app.vps_scraper_tree = _FakeTreeview()
        app.vps_scraper_records_by_key = {}
        app.selected_vps_scraper_key = None
        app.settings_window = None
        app.status_bar = _FakeStatusBar()
        app.vps_proxy_profile_options = {}
        app._load_local_proxy_profiles = lambda: []
        app.vps_scraper_form_vars = {
            "proxy_profile": _FakeVar("Custom (manual proxy)"),
            "proxy_profile_widget": {},
        }
        return app

    @staticmethod
    def _form_vars() -> dict[str, _FakeVar]:
        keys = (
            "worker_name",
            "route_name",
            "is_enabled",
            "priority",
            "lane_override",
            "route_interval_seconds",
            "user_data_dir",
            "proxy_mode",
            "proxy_server",
            "proxy_username",
            "proxy_password",
            "proxy_pool",
            "proxy_profile",
            "search_queries",
            "manual_login_required",
            "manual_login_reason",
            "quarantined_at",
            "quarantine_reason",
            "quarantine_evidence",
            "worker_status",
            "worker_scraped_last_minute",
            "worker_lease",
            "worker_cooldown",
            "computed_lane",
            "effective_lane",
            "priority_score",
            "priority_score_updated_at",
            "profitable_hit_rate",
            "recent_duplicate_ratio",
            "worker_last_run",
        )
        return {key: _FakeVar() for key in keys}

    def test_refresh_vps_scraper_tree_shows_health_rows_when_routes_absent(self) -> None:
        app = self._build_gui()
        app._fetch_vps_scraper_payloads = lambda: (
            [],
            {
                "worker_3": {
                    "worker_name": "worker_3",
                    "status": "ok",
                    "listings_scraped_last_minute": 12,
                    "updated_at": "2026-03-10T00:00:00+00:00",
                }
            },
            None,
        )

        app._refresh_vps_scraper_tree(preserve_selection=False)

        self.assertEqual(len(app.vps_scraper_records_by_key), 1)
        key = next(iter(app.vps_scraper_records_by_key))
        self.assertEqual(key, "worker_3::__health__::env_default")
        self.assertEqual(
            app.vps_scraper_tree.rows[key],
            ("worker_3", "env-backed", "env", "-", "ok", "12", "2026-03-10 00:00:00"),
        )
        self.assertTrue(app.vps_scraper_records_by_key[key]["route"]["is_synthetic_health_row"])
        self.assertIn("No saved VPS routes found", app.status_bar.text)

    def test_selecting_health_only_row_keeps_route_name_blank(self) -> None:
        app = self._build_gui()
        app.vps_scraper_form_vars = self._form_vars()
        app._fetch_vps_scraper_payloads = lambda: (
            [],
            {
                "worker_2": {
                    "worker_name": "worker_2",
                    "route_name": "iphone 15",
                    "status": "running",
                    "listings_scraped_last_minute": 7,
                    "last_run_finished_at": "2026-03-10T01:02:03+00:00",
                }
            },
            None,
        )

        app._refresh_vps_scraper_tree(preserve_selection=False)
        key = next(iter(app.vps_scraper_records_by_key))
        app.vps_scraper_tree.selection_set(key)

        app._on_vps_scraper_selected()

        self.assertEqual(app.vps_scraper_form_vars["worker_name"].get(), "worker_2")
        self.assertEqual(app.vps_scraper_form_vars["route_name"].get(), "iphone 15")
        self.assertEqual(app.vps_scraper_form_vars["priority"].get(), "")
        self.assertEqual(app.vps_scraper_form_vars["user_data_dir"].get(), "/app/runtime/browser_profile_2")
        self.assertEqual(app.vps_scraper_form_vars["worker_status"].get(), "running")
        self.assertEqual(app.vps_scraper_form_vars["priority_score"].get(), "")
        self.assertIn("route 'iphone 15'", app.status_bar.text)
        self.assertIn("Create or save a DB-backed route", app.status_bar.text)


if __name__ == "__main__":
    unittest.main()
