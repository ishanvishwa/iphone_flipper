import unittest
import sys
import types
from unittest.mock import patch


def _install_stub_module(name: str, **attrs) -> None:
    module = types.ModuleType(name)
    for key, value in attrs.items():
        setattr(module, key, value)
    sys.modules.setdefault(name, module)


_install_stub_module("desktop_sync", DesktopSyncCoordinator=object)
_install_stub_module(
    "deal_tracker",
    init_deal_tracking_tables=lambda: None,
    update_listing_conversion_scores=lambda: None,
)
_install_stub_module(
    "negotiation_agent",
    generate_response=lambda *args, **kwargs: "",
    generate_initial_message=lambda *args, **kwargs: "",
    analyze_conversation=lambda *args, **kwargs: {},
)
_install_stub_module(
    "scraper",
    ACCESSORY_SIGNAL_KEYWORDS=set(),
    SEARCH_QUERIES=[],
    init_db=lambda: None,
    purge_accessory_only_listings=lambda *args, **kwargs: 0,
    scrape_marketplace=lambda *args, **kwargs: [],
    recalculate_listing_financials=lambda *args, **kwargs: 0,
    update_fb_account_runtime_status=lambda *args, **kwargs: None,
)
_install_stub_module("notifications", notify_new_listings=lambda *args, **kwargs: None)
_install_stub_module(
    "requests",
    get=lambda *args, **kwargs: None,
    post=lambda *args, **kwargs: None,
    put=lambda *args, **kwargs: None,
    delete=lambda *args, **kwargs: None,
)

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
        self.text: dict[str, str] = {}
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

    def selection_remove(self, _item_ids) -> None:
        self.selected = ()

    def focus(self, item_id: str | None = None) -> str:
        if item_id is None:
            return self.focused
        self.focused = item_id
        return self.focused

    def see(self, item_id: str) -> None:
        self.seen = item_id

    def get_children(self) -> tuple[str, ...]:
        return tuple(self.rows)

    def insert(self, _parent: str, _index: str, iid: str, values: tuple[str, ...], text: str = "") -> None:
        self.rows[iid] = values
        self.text[iid] = text

    def delete(self, item_id: str) -> None:
        self.rows.pop(item_id, None)
        self.text.pop(item_id, None)

    def item(self, item_id: str, option: str | None = None, **kwargs):
        if "values" in kwargs:
            self.rows[item_id] = tuple(kwargs["values"])
        if option == "values":
            return self.rows[item_id]
        return {"values": self.rows[item_id], "text": self.text.get(item_id, "")}


class _FakeStatusBar:
    def __init__(self) -> None:
        self.text = ""

    def config(self, *, text: str) -> None:
        self.text = text


class GuiVpsScraperFallbackTests(unittest.TestCase):
    def _build_gui(self) -> gui.iPhoneFlipperGUI:
        app = gui.iPhoneFlipperGUI.__new__(gui.iPhoneFlipperGUI)
        app.root = object()
        app.vps_scraper_tree = _FakeTreeview()
        app.vps_worker_summary_tree = _FakeTreeview()
        app.dolphin_tree = _FakeTreeview()
        app.vps_scraper_records_by_key = {}
        app.vps_worker_summary_records_by_key = {}
        app.dolphin_profile_records_by_id = {}
        app.dolphin_tree_columns = (
            "ID",
            "Name",
            "Worker",
            "Runtime Profile",
            "Worker Mode",
            "Live Status",
            "Manual Login",
            "Dolphin Status",
            "Browser",
            "Tags",
            "Memory",
        )
        app.dolphin_tree_column_index = {
            column_name: index for index, column_name in enumerate(app.dolphin_tree_columns)
        }
        app.selected_vps_scraper_key = None
        app.query_manager_window = None
        app.query_manager_variant_tree = None
        app.query_manager_legacy_tree = None
        app.query_manager_family_form_vars = {}
        app.query_manager_family_variants = []
        app.query_manager_family_original_name = None
        app.settings_window = None
        app.status_bar = _FakeStatusBar()
        app.vps_proxy_profile_options = {}
        app._load_local_proxy_profiles = lambda: []
        app.vps_scraper_form_vars = self._form_vars()
        app.vps_scraper_form_vars["proxy_profile"].set("Custom (manual proxy)")
        app.vps_scraper_form_vars["proxy_profile_widget"] = {}
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
            ("worker_3", "env_default", "env-backed", "env", "-", "ok", "12", "2026-03-10 00:00:00"),
        )
        self.assertEqual(
            app.vps_worker_summary_tree.rows["worker_3"],
            ("worker_3", "env", "-", "env-backed", "-", "ok", "12", "2026-03-10 00:00:00"),
        )
        self.assertTrue(app.vps_scraper_records_by_key[key]["route"]["is_synthetic_health_row"])
        self.assertIn("No saved central routes found", app.status_bar.text)

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
                    "route_source": "env",
                    "route_user_data_dir": "/app/runtime/browser_profile_2",
                    "route_search_queries": "iPhone 15",
                    "route_proxy_mode": "fixed",
                    "route_proxy_server": "",
                    "route_proxy_username": "",
                    "route_proxy_password": "",
                    "route_proxy_pool": "",
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
        self.assertEqual(app.vps_scraper_form_vars["search_queries"].get(), "iPhone 15")
        self.assertEqual(app.vps_scraper_form_vars["proxy_profile"].get(), gui.VPS_PROXY_PROFILE_DIRECT)
        self.assertEqual(app.vps_scraper_form_vars["worker_status"].get(), "running")
        self.assertEqual(app.vps_scraper_form_vars["priority_score"].get(), "")
        self.assertIn("route 'iphone 15'", app.status_bar.text)
        self.assertIn("Create or save a DB-backed route", app.status_bar.text)

    def test_save_vps_scraper_route_allows_profile_bound_proxy_mode(self) -> None:
        app = self._build_gui()
        app.selected_vps_scraper_key = "worker_3::__health__::env_default"
        app._suggest_next_worker_name = lambda: "worker_3"
        app._normalize_worker_name_for_rotation = lambda worker_name: (worker_name, False)
        app._suggest_profile_dir_for_worker = lambda worker_name: "/app/runtime/browser_profile_3"
        app._get_server_api_context = lambda: ({"base_url": "https://example.com", "headers": {"x-api-token": "test"}}, None)
        app._refresh_vps_scraper_tree = lambda preserve_selection=True: None
        app._show_vps_route_warnings = lambda warnings: None
        app.vps_scraper_form_vars["worker_name"].set("worker_3")
        app.vps_scraper_form_vars["route_name"].set("env_default")
        app.vps_scraper_form_vars["is_enabled"].set("1")
        app.vps_scraper_form_vars["priority"].set("100")
        app.vps_scraper_form_vars["lane_override"].set("hot")
        app.vps_scraper_form_vars["route_interval_seconds"].set("")
        app.vps_scraper_form_vars["user_data_dir"].set("/app/runtime/browser_profile_3")
        app.vps_scraper_form_vars["proxy_mode"].set("fixed")
        app.vps_scraper_form_vars["proxy_profile"].set(gui.VPS_PROXY_PROFILE_DIRECT)
        app.vps_scraper_form_vars["proxy_server"].set("")
        app.vps_scraper_form_vars["proxy_username"].set("")
        app.vps_scraper_form_vars["proxy_password"].set("")
        app.vps_scraper_form_vars["proxy_pool"].set("")
        app.vps_scraper_form_vars["search_queries"].set("iPhone")
        app.vps_scraper_form_vars["manual_login_required"].set("0")
        app.vps_scraper_form_vars["manual_login_reason"].set("")

        class _Response:
            status_code = 200
            content = b'{"ok": true}'

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"ok": True}

        with patch.object(gui.requests, "put", return_value=_Response()) as put_mock:
            saved = app._save_vps_scraper_route(allow_remap=False)

        self.assertTrue(saved)
        self.assertIsNone(put_mock.call_args.kwargs["json"]["proxy_server"])

    def test_refresh_populates_worker_summary_and_route_first_table(self) -> None:
        app = self._build_gui()
        app._fetch_vps_scraper_payloads = lambda: (
            [
                {
                    "legacy_worker_name": "worker",
                    "route_name": "iphone_16_hot",
                    "search_queries": "iPhone 16 Pro, iPhone 16 Pro Max",
                    "effective_lane": "hot",
                    "computed_lane": "hot",
                    "priority_score": 6.42,
                    "priority": 10,
                    "is_enabled": True,
                    "lane_override": "",
                    "manual_login_required": False,
                    "cooldown_until": None,
                },
                {
                    "legacy_worker_name": "worker",
                    "route_name": "iphone_misc_sweep",
                    "search_queries": "iPhone",
                    "effective_lane": "sweep",
                    "computed_lane": "sweep",
                    "priority_score": 1.75,
                    "priority": 100,
                    "is_enabled": True,
                    "lane_override": "",
                    "manual_login_required": False,
                    "cooldown_until": None,
                },
            ],
            {
                "worker": {
                    "worker_name": "worker",
                    "route_name": "iphone_16_hot",
                    "status": "ok",
                    "listings_scraped_last_minute": 19,
                    "last_run_finished_at": "2026-03-10T02:03:04+00:00",
                }
            },
            None,
        )

        app._refresh_vps_scraper_tree(preserve_selection=False)

        self.assertEqual(
            app.vps_scraper_tree.rows["route::iphone_16_hot"],
            ("worker", "iphone_16_hot", "2 query(s)", "hot", "6.42", "ok", "19", "2026-03-10 02:03:04"),
        )
        self.assertEqual(
            app.vps_scraper_tree.rows["route::iphone_misc_sweep"],
            ("worker", "iphone_misc_sweep", "1 query(s)", "sweep", "1.75", "enabled", "-", ""),
        )
        self.assertEqual(
            app.vps_worker_summary_tree.rows["worker"],
            ("worker", "central", "-", "iphone_16_hot", "iPhone 16 Pro, iPhone 16 Pro Max", "ok", "19", "2026-03-10 02:03:04"),
        )

        app.vps_worker_summary_tree.selection_set("worker")
        app._on_vps_worker_summary_selected()
        self.assertEqual(app.vps_scraper_tree.selection(), ("route::iphone_16_hot",))

    def test_fetch_vps_scraper_payloads_uses_central_routes_endpoint(self) -> None:
        app = self._build_gui()
        app._get_server_api_context = lambda: (
            {"base_url": "https://example.com", "headers": {"x-api-token": "test", "Content-Type": "application/json"}},
            None,
        )

        class _Response:
            def __init__(self, status_code: int, payload: dict | None = None) -> None:
                self.status_code = status_code
                self._payload = payload or {}
                self.content = b"{}"

            def raise_for_status(self) -> None:
                if self.status_code >= 400:
                    raise RuntimeError(f"http {self.status_code}")

            def json(self) -> dict:
                return dict(self._payload)

        route_response = _Response(
            200,
            {
                "items": [
                    {
                        "legacy_worker_name": "worker_3",
                        "route_name": "env_default",
                        "search_queries": "iPhone",
                        "effective_lane": "warm",
                        "computed_lane": "warm",
                        "priority_score": None,
                        "priority": 100,
                        "is_enabled": True,
                        "lane_override": "",
                    }
                ]
            },
        )
        health_response = _Response(
            200,
            {
                "items": [
                    {
                        "worker_name": "worker_3",
                        "route_name": "env_default",
                        "route_source": "env",
                        "route_user_data_dir": "/app/runtime/browser_profile_3",
                        "route_search_queries": "iPhone",
                        "route_proxy_mode": "fixed",
                        "route_proxy_server": "",
                        "route_proxy_username": "",
                        "route_proxy_password": "",
                        "route_proxy_pool": "",
                        "status": "ok",
                    }
                ]
            },
        )
        def _fake_get(url: str, **_kwargs):
            if url.endswith("/routes"):
                return route_response
            if url.endswith("/worker-health"):
                return health_response
            raise AssertionError(f"Unexpected GET {url}")

        with patch.object(gui.requests, "get", side_effect=_fake_get) as get_mock:
            routes, health_by_worker, error = gui.iPhoneFlipperGUI._fetch_vps_scraper_payloads(app)

        self.assertIsNone(error)
        self.assertEqual(len(routes), 1)
        self.assertEqual(routes[0]["legacy_worker_name"], "worker_3")
        self.assertIn("worker_3", health_by_worker)
        self.assertEqual(get_mock.call_count, 2)

    def test_refresh_query_manager_routes_loads_v4_families_and_legacy_routes(self) -> None:
        app = self._build_gui()
        app.query_manager_tree = _FakeTreeview()
        app.query_manager_variant_tree = _FakeTreeview()
        app.query_manager_legacy_tree = _FakeTreeview()
        app.query_manager_family_form_vars = {
            "name": _FakeVar(),
            "is_enabled": _FakeVar(),
            "lane": _FakeVar(),
            "priority": _FakeVar(),
            "min_gap_s": _FakeVar(),
        }
        app._fetch_query_family_payloads = lambda: (
            [
                {
                    "name": "iphone_broad",
                    "is_enabled": True,
                    "lane": "hot",
                    "priority": 300,
                    "min_gap_s": 5,
                    "validated_variant_count": 1,
                    "active_worker_name": "worker_3",
                    "last_discovery_at": "2026-03-10T03:04:05+00:00",
                    "variants": [
                        {
                            "query_text": "iPhone",
                            "validation_state": "validated",
                            "is_enabled": True,
                            "weight": 1.0,
                            "notes": "broad",
                        }
                    ],
                }
            ],
            None,
        )
        app._fetch_vps_scraper_payloads = lambda: (
            [
                {
                    "legacy_worker_name": "worker",
                    "route_name": "env_default",
                    "search_queries": "iPhone 15 Pro",
                    "effective_lane": "warm",
                    "computed_lane": "warm",
                    "status": "ENABLED",
                    "priority": 100,
                }
            ],
            {},
            None,
        )

        app._refresh_query_manager_routes()

        self.assertEqual(
            app.query_manager_tree.rows["family::iphone_broad"],
            ("iphone_broad", "Yes", "hot", "5", "1/1", "worker_3", "2026-03-10 03:04:05"),
        )
        self.assertEqual(app.query_manager_family_form_vars["name"].get(), "iphone_broad")
        self.assertEqual(app.query_manager_variant_tree.rows["variant::0"], ("iPhone", "validated", "Yes", "1", "broad"))
        self.assertEqual(
            app.query_manager_legacy_tree.rows["legacy::env_default"],
            ("worker", "env_default", "warm", "iPhone 15 Pro", "ENABLED"),
        )

    def test_save_query_manager_route_queries_uses_v4_family_endpoint(self) -> None:
        app = self._build_gui()
        app.query_manager_tree = _FakeTreeview()
        app.query_manager_tree.insert("", "end", iid="family::iphone_hot", values=())
        app.query_manager_tree.selection_set("family::iphone_hot")
        app.query_manager_routes_by_key = {
            "family::iphone_hot": {
                "family": {
                    "name": "iphone_hot",
                    "legacy_route_name": "worker_3__env_default",
                    "legacy_worker_name": "worker_3",
                    "max_gap_s": None,
                }
            }
        }
        app.query_manager_family_form_vars = {
            "name": _FakeVar("iphone_hot"),
            "is_enabled": _FakeVar("1"),
            "lane": _FakeVar("warm"),
            "priority": _FakeVar("220"),
            "min_gap_s": _FakeVar("15"),
        }
        app.query_manager_family_variants = [
            {
                "query_text": "iPhone 15",
                "validation_state": "validated",
                "is_enabled": True,
                "weight": 1.0,
                "notes": "primary",
            },
            {
                "query_text": "iPhone 15 Pro",
                "validation_state": "pending_validation",
                "is_enabled": False,
                "weight": 0.5,
                "notes": "staged",
            },
        ]
        app.query_manager_family_original_name = "iphone_hot"
        app._get_server_api_context = lambda: (
            {"base_url": "https://example.com", "headers": {"x-api-token": "test", "Content-Type": "application/json"}},
            None,
        )
        app._refresh_query_manager_routes = lambda preferred_family_name=None: None

        class _Response:
            status_code = 200

            def raise_for_status(self) -> None:
                return None

        with patch.object(gui.requests, "put", return_value=_Response()) as put_mock:
            app._save_query_manager_route_queries()

        self.assertEqual(
            put_mock.call_args.args[0],
            "https://example.com/query-families/iphone_hot",
        )
        self.assertEqual(
            put_mock.call_args.kwargs["json"],
            {
                "previous_name": "iphone_hot",
                "is_enabled": True,
                "priority": 220,
                "lane": "warm",
                "min_gap_s": 15,
                "max_gap_s": None,
                "legacy_route_name": "worker_3__env_default",
                "legacy_worker_name": "worker_3",
                "variants": [
                    {
                        "query_text": "iPhone 15",
                        "validation_state": "validated",
                        "is_enabled": True,
                        "weight": 1.0,
                        "notes": "primary",
                    },
                    {
                        "query_text": "iPhone 15 Pro",
                        "validation_state": "pending_validation",
                        "is_enabled": False,
                        "weight": 0.5,
                        "notes": "staged",
                    },
                ],
            },
        )

    def test_delete_query_manager_family_uses_v4_family_delete_endpoint(self) -> None:
        app = self._build_gui()
        app.query_manager_tree = _FakeTreeview()
        app.query_manager_tree.insert("", "end", iid="family::iphone_hot", values=())
        app.query_manager_tree.selection_set("family::iphone_hot")
        app.query_manager_routes_by_key = {
            "family::iphone_hot": {
                "family": {
                    "name": "iphone_hot",
                }
            }
        }
        app.query_manager_family_form_vars = {
            "name": _FakeVar("iphone_hot"),
        }
        app.query_manager_family_original_name = "iphone_hot"
        app._get_server_api_context = lambda: (
            {"base_url": "https://example.com", "headers": {"x-api-token": "test", "Content-Type": "application/json"}},
            None,
        )
        refresh_calls: list[str | None] = []
        app._refresh_query_manager_routes = lambda preferred_family_name=None: refresh_calls.append(preferred_family_name)

        class _Response:
            status_code = 200
            content = b'{"ok": true, "deleted": true}'

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict[str, bool]:
                return {"ok": True, "deleted": True}

        with (
            patch.object(gui.messagebox, "askyesno", return_value=True),
            patch.object(gui.requests, "delete", return_value=_Response()) as delete_mock,
        ):
            deleted = app._delete_query_manager_family()

        self.assertTrue(deleted)
        self.assertEqual(
            delete_mock.call_args.args[0],
            "https://example.com/query-families/iphone_hot",
        )
        self.assertEqual(refresh_calls, [None])

    def test_refresh_dolphin_profiles_populates_live_vps_status_columns(self) -> None:
        app = self._build_gui()
        app._dolphin_auth_headers = lambda: {"Authorization": "Bearer test-token"}
        app._fetch_execution_profile_payloads = lambda: (
            [
                {
                    "worker_name": "worker_3",
                    "user_data_dir": "/app/runtime/browser_profile_3",
                    "dolphin_profile_name": "Profile 3",
                    "status": "NEEDS_LOGIN",
                    "status_reason": "checkpoint",
                    "manual_login_required": True,
                    "manual_login_reason": "checkpoint",
                }
            ],
            None,
        )
        app._fetch_vps_worker_health = lambda: ({"worker_3": {"route_source": "v4"}}, None)

        class _Response:
            status_code = 200
            content = b'{"data": []}'

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {
                    "data": [
                        {
                            "id": 746386753,
                            "name": "Profile 3",
                            "tags": ["worker_3"],
                            "browserType": "chromium",
                            "memory": {"value": "2 GB"},
                            "status": {"name": "Ready"},
                        }
                    ]
                }

        with patch.object(gui.requests, "get", return_value=_Response()):
            app._refresh_dolphin_profiles()

        row = app.dolphin_tree.rows["746386753"]
        self.assertEqual(row[2], "worker_3")
        self.assertEqual(row[3], "Profile 3")
        self.assertEqual(row[4], "v4")
        self.assertEqual(row[6], "Required")
        self.assertIn("NEEDS_LOGIN", row[5])
        self.assertEqual(row[7], "Ready")

    def test_refresh_vps_worker_summary_prefers_live_profile_name(self) -> None:
        app = self._build_gui()

        app._refresh_vps_worker_summary_tree(
            routes=[],
            health_by_worker={
                "worker_3": {
                    "worker_name": "worker_3",
                    "route_source": "v4",
                    "route_name": "iphone_broad",
                    "route_user_data_dir": "/app/runtime/browser_profile_3",
                    "route_profile_name": "Profile 3",
                    "route_search_queries": "iPhone",
                    "status": "running",
                    "listings_scraped_last_minute": 12,
                    "updated_at": "2026-03-13T10:20:30+00:00",
                }
            },
        )

        row = app.vps_worker_summary_tree.rows["worker_3"]
        self.assertEqual(row[2], "Profile 3")
        self.assertEqual(row[3], "iphone_broad")

    def test_clear_selected_dolphin_manual_login_calls_execution_profile_endpoint(self) -> None:
        app = self._build_gui()
        app.dolphin_tree.insert(
            "",
            "end",
            iid="746386753",
            values=("746386753", "Profile 3", "worker_3", "browser_profile_3", "v4", "NEEDS_LOGIN", "Required", "Ready", "chromium", "", "2 GB"),
        )
        app.dolphin_tree.selection_set("746386753")
        app.dolphin_profile_records_by_id = {
            "746386753": {
                "cloud_profile": {"name": "Profile 3"},
                "execution_profile": {
                    "worker_name": "worker_3",
                    "user_data_dir": "/app/runtime/browser_profile_3",
                    "dolphin_profile_name": "Profile 3",
                }
            }
        }
        app._get_server_api_context = lambda: (
            {"base_url": "https://example.com", "headers": {"x-api-token": "test", "Content-Type": "application/json"}},
            None,
        )
        app._refresh_dolphin_profiles = lambda: None
        app._refresh_vps_scraper_tree = lambda preserve_selection=True: None

        class _Response:
            status_code = 200
            content = b'{"ok": true}'

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"ok": True}

        with (
            patch.object(gui.messagebox, "askyesno", return_value=True),
            patch.object(gui.simpledialog, "askstring", return_value="login fixed"),
            patch.object(gui.messagebox, "showerror"),
            patch.object(gui.requests, "post", return_value=_Response()) as post_mock,
        ):
            app._clear_selected_dolphin_manual_login()

        self.assertEqual(
            post_mock.call_args.args[0],
            "https://example.com/execution-profiles/worker_3/clear-manual-login",
        )
        self.assertEqual(
            post_mock.call_args.kwargs["json"],
            {
                "user_data_dir": "/app/runtime/browser_profile_3",
                "reason": "login fixed",
            },
        )
        self.assertIn("Cleared manual login on 1 profile", app.status_bar.text)

    def test_sync_all_dolphin_profiles_to_execution_pool_posts_detected_slots(self) -> None:
        app = self._build_gui()
        app.dolphin_profile_records_by_id = {
            "100": {
                "cloud_profile": {
                    "id": 100,
                    "name": "browser_profile_4",
                    "tags": ["worker_4"],
                }
            },
            "101": {
                "cloud_profile": {
                    "id": 101,
                    "name": "Seller Browser",
                    "tags": ["browser_profile_5", "warm"],
                }
            },
            "102": {
                "cloud_profile": {
                    "id": 102,
                    "name": "No slot",
                    "tags": [],
                }
            },
        }
        app._get_server_api_context = lambda: (
            {"base_url": "https://example.com", "headers": {"x-api-token": "test", "Content-Type": "application/json"}},
            None,
        )
        refresh_calls: list[str] = []
        app._refresh_dolphin_profiles = lambda: refresh_calls.append("dolphin")
        app._refresh_vps_scraper_tree = lambda preserve_selection=True: refresh_calls.append("vps")

        class _Response:
            status_code = 200
            content = b'{"ok": true, "synced_count": 2, "slots": [4, 5], "items": []}'

            def raise_for_status(self) -> None:
                return None

            def json(self) -> dict:
                return {"ok": True, "synced_count": 2, "slots": [4, 5], "items": []}

        with (
            patch.object(gui.messagebox, "askyesno", return_value=True),
            patch.object(gui.requests, "post", return_value=_Response()) as post_mock,
        ):
            app._sync_all_dolphin_profiles_to_execution_pool()

        self.assertEqual(
            post_mock.call_args.args[0],
            "https://example.com/execution-profiles/sync-pool",
        )
        self.assertEqual(
            post_mock.call_args.kwargs["json"],
            {
                "slots": [4, 5],
                "profiles": [
                    {
                        "slot": 4,
                        "dolphin_profile_id": "100",
                        "dolphin_profile_name": "browser_profile_4",
                    },
                    {
                        "slot": 5,
                        "dolphin_profile_id": "101",
                        "dolphin_profile_name": "Seller Browser",
                    },
                ],
            },
        )
        self.assertEqual(refresh_calls, ["dolphin", "vps"])
        self.assertIn("Synced 2 Dolphin profile slot(s)", app.status_bar.text)


if __name__ == "__main__":
    unittest.main()
