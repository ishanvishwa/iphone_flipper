"""
iPhone Flipper GUI Application

A user-friendly graphical interface for managing iPhone flipping automation.
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog, filedialog
import sqlite3
import webbrowser
import threading
import asyncio
import csv
import queue
from contextlib import suppress
from pathlib import Path
from datetime import datetime
import subprocess
import sys
import json
import random
import re
import socket
import importlib.util
import time
import shlex
from urllib.parse import urlparse, quote

try:
    import requests
except ImportError:
    requests = None

import desktop_sync

# Import our modules
import deal_tracker
from negotiation_agent import generate_response, generate_initial_message, analyze_conversation
from scraper import (
    ACCESSORY_SIGNAL_KEYWORDS,
    init_db,
    purge_accessory_only_listings,
    scrape_marketplace,
    recalculate_listing_financials,
    SEARCH_QUERIES,
    update_fb_account_runtime_status,
)
from notifications import notify_new_listings
from server.services.common.observability import emit_json_log, utc_now_iso

DB_PATH = Path(__file__).parent / "listings.db"
VPS_PROXY_PROFILE_DIRECT = "Use Dolphin/Profile Proxy"
VPS_PROXY_PROFILE_CUSTOM = "Custom (manual proxy)"


class iPhoneFlipperGUI:
    def __init__(self, root):
        self.root = root
        self.root.title("iPhone Flipper Pro - Perth")
        self.root.geometry("1400x900")
        self.root.configure(bg="#f0f0f0")
        
        # Initialize database and deal tracking
        init_db()  # Create listings table first
        deal_tracker.init_deal_tracking_tables()  # Then add deal tracking tables

        # Scraper runtime state
        self.scraper_thread = None
        self.scraper_stop_event = None
        self.scraper_started_at = None
        self.scraper_result_data = None

        # Price sheet editor state
        self.price_sheet_window = None
        self.price_sheet_tree = None
        self.price_sheet_rows = []
        self.price_sheet_selected_index = None
        self.price_form_vars = {}

        # Settings editor state
        self.settings_window = None
        self.server_monitor_window = None
        self.server_monitor_text = None
        self.server_log_stream_process = None
        self.server_log_stream_thread = None
        self.query_manager_window = None
        self.query_manager_tree = None
        self.query_manager_routes_by_key = {}
        self.query_manager_query_var = None
        self.query_manager_negative_keywords_var = None
        self.query_manager_accessory_max_price_var = None
        self.account_tree = None
        self.proxy_tree = None
        self.vps_scraper_tree = None
        self.vps_worker_summary_tree = None
        self.account_form_vars = {}
        self.proxy_form_vars = {}
        self.vps_scraper_form_vars = {}
        self.scraper_setting_vars = {}
        self.selected_account_id = None
        self.selected_proxy_id = None
        self.selected_vps_scraper_key = None
        self.proxy_options_by_id = {}
        self.vps_scraper_records_by_key = {}
        self.vps_worker_summary_records_by_key = {}
        self.vps_proxy_profile_options = {}
        self.active_scraper_account_var = None
        self.server_sync_thread = None
        self.server_sync_stop_event = None
        self.server_sync_last_ui_refresh = 0.0
        self.server_sync_coordinator = None
        self.server_sync_event_queue = queue.Queue()
        self.server_sync_event_job = None
        self.worker_status_labels = {}
        self.worker_status_canvases = {}
        self.worker_status_dot_ids = {}
        self.worker_status_poll_job = None
        self.worker_status_poll_inflight = False
        self.worker_status_poll_interval_seconds = 5
        self.worker_status_polling_active = False
        self.listing_drag_anchor = None

        # Create main container
        self.create_menu()
        self.create_main_layout()
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # Load initial data
        self.refresh_listings()
        self.refresh_negotiation_listings()
        self.refresh_deals()
        self._start_or_restart_server_sync_from_settings()
        self._start_worker_status_polling()
        
    def create_menu(self):
        """Create the menu bar."""
        menubar = tk.Menu(self.root)
        self.root.config(menu=menubar)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="File", menu=file_menu)
        file_menu.add_command(label="Refresh Listings", command=self.refresh_listings)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        
        # Scraper menu
        scraper_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Scraper", menu=scraper_menu)
        scraper_menu.add_command(label="Run Scraper Now", command=self.run_scraper)
        scraper_menu.add_command(label="Start Monitor (Saved Interval)", command=self.start_monitor)
        scraper_menu.add_command(label="VPS Activity Monitor", command=self.show_server_activity_monitor)
        scraper_menu.add_command(label="Worker Queries & Keywords", command=self.show_query_keyword_manager)
        
        # Analytics menu
        analytics_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Analytics", menu=analytics_menu)
        analytics_menu.add_command(label="View Patterns", command=self.show_patterns)
        analytics_menu.add_command(label="Purchase History", command=self.show_history)
        analytics_menu.add_command(label="Insights", command=self.show_insights)

        # Price sheet menu
        price_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Price Sheet", menu=price_menu)
        price_menu.add_command(label="Edit Price Sheet", command=self.show_price_sheet_editor)
        price_menu.add_command(label="Recalculate Listing Values", command=self.recalculate_all_listing_values)

        # Settings menu
        settings_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Settings", menu=settings_menu)
        settings_menu.add_command(label="Connections & Scraper", command=self.show_settings_manager)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        menubar.add_cascade(label="Help", menu=help_menu)
        help_menu.add_command(label="Setup & Launch Guide", command=self.show_launch_guide)
        help_menu.add_command(label="About", command=self.show_about)
        
    def create_main_layout(self):
        """Create the main layout with tabs."""
        # Create notebook (tabbed interface)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Tab 1: Listings Browser
        self.listings_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.listings_frame, text="📱 Listings")
        self.create_listings_tab()
        
        # Tab 2: Negotiation
        self.negotiation_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.negotiation_frame, text="💬 Negotiation")
        self.create_negotiation_tab()
        
        # Tab 3: Deals
        self.deals_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.deals_frame, text="✅ Deals")
        self.create_deals_tab()
        
        # Status bar at bottom
        self.create_status_bar()
        
    def create_listings_tab(self):
        """Create the listings browser tab."""
        # Top controls
        controls_frame = ttk.Frame(self.listings_frame)
        controls_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(controls_frame, text="Filter:").pack(side=tk.LEFT, padx=5)
        
        self.filter_var = tk.StringVar(value="all")
        filters = [
            ("All", "all"),
            ("New", "new"),
            ("High Profit (>$100)", "high_profit"),
            ("iPhone 14+", "iphone14plus")
        ]
        
        for text, value in filters:
            ttk.Radiobutton(
                controls_frame, 
                text=text, 
                variable=self.filter_var, 
                value=value,
                command=self.refresh_listings
            ).pack(side=tk.LEFT, padx=5)

        ttk.Label(controls_frame, text="Models:").pack(side=tk.LEFT, padx=(12, 4))
        self.model_filter_button_text = tk.StringVar(value="All Models")
        self.model_filter_selected_models = set()
        self.model_filter_vars = {}
        self.model_filter_button = ttk.Menubutton(
            controls_frame,
            textvariable=self.model_filter_button_text,
            direction="below",
        )
        self.model_filter_button.pack(side=tk.LEFT, padx=4)
        self.model_filter_menu = tk.Menu(self.model_filter_button, tearoff=0)
        self.model_filter_button["menu"] = self.model_filter_menu

        self.min_profit_filter_var = tk.StringVar()
        self.max_profit_filter_var = tk.StringVar()
        self.min_price_filter_var = tk.StringVar()

        ttk.Label(controls_frame, text="Profit:").pack(side=tk.LEFT, padx=(12, 4))
        min_profit_entry = ttk.Entry(controls_frame, textvariable=self.min_profit_filter_var, width=7)
        min_profit_entry.pack(side=tk.LEFT, padx=(0, 2))
        ttk.Label(controls_frame, text="to").pack(side=tk.LEFT, padx=2)
        max_profit_entry = ttk.Entry(controls_frame, textvariable=self.max_profit_filter_var, width=7)
        max_profit_entry.pack(side=tk.LEFT, padx=(2, 6))

        ttk.Label(controls_frame, text="Min Price:").pack(side=tk.LEFT, padx=(8, 4))
        min_price_entry = ttk.Entry(controls_frame, textvariable=self.min_price_filter_var, width=7)
        min_price_entry.pack(side=tk.LEFT, padx=(0, 6))

        for entry in (min_profit_entry, max_profit_entry, min_price_entry):
            entry.bind("<Return>", lambda _event: self.refresh_listings())

        ttk.Button(
            controls_frame,
            text="Clear Filters",
            command=self.clear_advanced_listing_filters,
        ).pack(side=tk.LEFT, padx=4)
        
        self.refresh_listings_button = ttk.Button(
            controls_frame,
            text="🔄 Refresh",
            command=self.refresh_listings
        )
        self.refresh_listings_button.pack(side=tk.RIGHT, padx=5)



        worker_status_frame = ttk.Frame(self.listings_frame)
        worker_status_frame.pack(fill=tk.X, padx=10, pady=(0, 6))
        ttk.Label(worker_status_frame, text="VPS Workers:").pack(side=tk.LEFT, padx=(0, 8))

        worker_specs = [
            ("worker", "Worker 1"),
            ("worker_2", "Worker 2"),
            ("worker_3", "Worker 3"),
        ]
        for worker_name, worker_label in worker_specs:
            pill = ttk.Frame(worker_status_frame)
            pill.pack(side=tk.LEFT, padx=(0, 10))
            canvas = tk.Canvas(pill, width=14, height=14, highlightthickness=0, bd=0)
            dot_id = canvas.create_oval(2, 2, 12, 12, fill="#d93025", outline="#d93025")
            canvas.pack(side=tk.LEFT, padx=(0, 4))
            label = ttk.Label(pill, text=f"{worker_label}: offline")
            label.pack(side=tk.LEFT)
            self.worker_status_labels[worker_name] = label
            self.worker_status_canvases[worker_name] = canvas
            self.worker_status_dot_ids[worker_name] = dot_id
        
        # Listings table
        table_frame = ttk.Frame(self.listings_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Scrollbars
        vsb = ttk.Scrollbar(table_frame, orient="vertical")
        hsb = ttk.Scrollbar(table_frame, orient="horizontal")
        
        # Treeview
        columns = ("ID", "Model", "Price", "Max Buy", "Profit", "Condition", "Score", "Status")
        self.listings_tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            selectmode="extended",
            yscrollcommand=vsb.set,
            xscrollcommand=hsb.set
        )
        
        vsb.config(command=self.listings_tree.yview)
        hsb.config(command=self.listings_tree.xview)
        
        # Column headings
        self.listings_tree.heading("ID", text="Listing ID")
        self.listings_tree.heading("Model", text="Model")
        self.listings_tree.heading("Price", text="Price")
        self.listings_tree.heading("Max Buy", text="Max Buy")
        self.listings_tree.heading("Profit", text="Profit")
        self.listings_tree.heading("Condition", text="Condition")
        self.listings_tree.heading("Score", text="Conv. Score")
        self.listings_tree.heading("Status", text="Status")
        
        # Column widths
        self.listings_tree.column("ID", width=120)
        self.listings_tree.column("Model", width=150)
        self.listings_tree.column("Price", width=80)
        self.listings_tree.column("Max Buy", width=80)
        self.listings_tree.column("Profit", width=80)
        self.listings_tree.column("Condition", width=150)
        self.listings_tree.column("Score", width=80)
        self.listings_tree.column("Status", width=100)

        # Row color flags set via context menu.
        self.listings_tree.tag_configure("flag_scam", background="#ffe2e2", foreground="#8b0000")
        self.listings_tree.tag_configure("flag_interested", background="#fff9cc", foreground="#6d5d00")
        self.listings_tree.tag_configure("flag_not_interested", background="#ececec", foreground="#4f4f4f")
        self.listings_tree.tag_configure("flag_opened", background="#e8f1ff", foreground="#1f4f9a")
        
        # Grid layout
        self.listings_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        # Bind double-click to open listing
        self.listings_tree.bind("<Double-1>", self.on_listing_double_click)
        self.listings_tree.bind("<Button-1>", self._on_listing_left_click)
        self.listings_tree.bind("<B1-Motion>", self._on_listing_drag_select)
        
        # Context menu
        self.listings_tree.bind("<Button-3>", self.show_listing_context_menu)
        self.listings_tree.bind("<Button-2>", self.show_listing_context_menu)
        self.listings_tree.bind("<Control-Button-1>", self.show_listing_context_menu)
        self.listing_context_menu = tk.Menu(self.root, tearoff=0)
        self.listing_context_menu.add_command(
            label="🚩 Mark Scam (Red)",
            command=lambda: self.set_selected_listing_flag("scam"),
        )
        self.listing_context_menu.add_command(
            label="⭐ Mark Interested (Yellow)",
            command=lambda: self.set_selected_listing_flag("interested"),
        )
        self.listing_context_menu.add_command(
            label="⬜ Mark Not Interested (Ash)",
            command=lambda: self.set_selected_listing_flag("not_interested"),
        )
        self.listing_context_menu.add_separator()
        self.listing_context_menu.add_command(
            label="Clear Marking",
            command=self.clear_selected_listing_flag,
        )
        self.listing_context_menu.add_separator()
        self.listing_context_menu.add_command(
            label="🗑 Delete Selected",
            command=self.delete_selected_listings,
        )
        
        # Bottom buttons
        button_frame = ttk.Frame(self.listings_frame)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(
            button_frame,
            text="🌐 Open in Browser",
            command=self.open_selected_listing
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="💬 Start Negotiation",
            command=self.start_negotiation_from_listing
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="✅ Mark as Purchased",
            command=self.mark_as_purchased
        ).pack(side=tk.LEFT, padx=5)

        self._refresh_model_filter_menu()
        
    def create_negotiation_tab(self):
        """Create the negotiation interface tab."""
        # Top: Listing selector
        selector_frame = ttk.Frame(self.negotiation_frame)
        selector_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(selector_frame, text="Select Listing:").pack(side=tk.LEFT, padx=5)
        
        self.negotiation_listing_var = tk.StringVar()
        self.negotiation_listing_combo = ttk.Combobox(
            selector_frame,
            textvariable=self.negotiation_listing_var,
            width=60,
            state="readonly"
        )
        self.negotiation_listing_combo.pack(side=tk.LEFT, padx=5, fill=tk.X, expand=True)
        self.negotiation_listing_combo.bind("<<ComboboxSelected>>", self.load_conversation)
        
        ttk.Button(
            selector_frame,
            text="🔄 Refresh",
            command=self.refresh_negotiation_listings
        ).pack(side=tk.LEFT, padx=5)
        
        # Middle: Conversation history
        conv_frame = ttk.LabelFrame(self.negotiation_frame, text="Conversation History")
        conv_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        self.conversation_text = scrolledtext.ScrolledText(
            conv_frame,
            wrap=tk.WORD,
            width=80,
            height=20,
            font=("Arial", 10)
        )
        self.conversation_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Configure tags for styling
        self.conversation_text.tag_config("seller", foreground="blue", font=("Arial", 10, "bold"))
        self.conversation_text.tag_config("you", foreground="green", font=("Arial", 10, "bold"))
        self.conversation_text.tag_config("system", foreground="gray", font=("Arial", 9, "italic"))
        
        # Bottom: Input area
        input_frame = ttk.LabelFrame(self.negotiation_frame, text="Your Message")
        input_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.message_input = scrolledtext.ScrolledText(
            input_frame,
            wrap=tk.WORD,
            width=80,
            height=5,
            font=("Arial", 10)
        )
        self.message_input.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Buttons
        button_frame = ttk.Frame(input_frame)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(
            button_frame,
            text="🤖 Generate AI Response",
            command=self.generate_ai_response
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="📋 Copy to Clipboard",
            command=self.copy_message_to_clipboard
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="🔍 Analyze Conversation",
            command=self.analyze_current_conversation
        ).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            button_frame,
            text="✅ Mark as Purchased",
            command=self.mark_as_purchased_from_negotiation
        ).pack(side=tk.RIGHT, padx=5)
        
    def create_deals_tab(self):
        """Create the deals tracking tab."""
        # Top controls
        controls_frame = ttk.Frame(self.deals_frame)
        controls_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Label(controls_frame, text="Purchase History", font=("Arial", 14, "bold")).pack(side=tk.LEFT, padx=5)
        
        ttk.Button(
            controls_frame,
            text="🔄 Refresh",
            command=self.refresh_deals
        ).pack(side=tk.RIGHT, padx=5)
        
        # Deals table
        table_frame = ttk.Frame(self.deals_frame)
        table_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        vsb = ttk.Scrollbar(table_frame, orient="vertical")
        
        columns = ("Date", "Model", "Bought For", "Sold For", "Repair Cost", "Profit", "Status")
        self.deals_tree = ttk.Treeview(
            table_frame,
            columns=columns,
            show="headings",
            yscrollcommand=vsb.set
        )
        
        vsb.config(command=self.deals_tree.yview)
        
        # Column headings
        for col in columns:
            self.deals_tree.heading(col, text=col)
            self.deals_tree.column(col, width=120)
        
        self.deals_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        
        table_frame.grid_rowconfigure(0, weight=1)
        table_frame.grid_columnconfigure(0, weight=1)
        
        # Summary frame
        summary_frame = ttk.LabelFrame(self.deals_frame, text="Summary")
        summary_frame.pack(fill=tk.X, padx=10, pady=10)
        
        self.summary_label = ttk.Label(
            summary_frame,
            text="No purchases yet",
            font=("Arial", 11)
        )
        self.summary_label.pack(padx=10, pady=10)
        
    def create_status_bar(self):
        """Create the status bar at the bottom."""
        self.status_bar = ttk.Label(
            self.root,
            text="Ready",
            relief=tk.SUNKEN,
            anchor=tk.W
        )
        self.status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def _on_close(self):
        """Stop background workers before exiting GUI."""
        self._stop_worker_status_polling()
        self._stop_server_log_stream(update_status=False)
        self._stop_server_sync()
        self.root.destroy()

    @staticmethod
    def _is_truthy(value: str) -> bool:
        return str(value).strip().lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _safe_int(value, default: int) -> int:
        try:
            return int(str(value).strip())
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _safe_float(value):
        if value is None or value == "":
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _normalize_server_base_url(raw_url: str) -> str:
        base = (raw_url or "").strip().rstrip("/")
        if not base:
            return ""
        if not base.startswith(("http://", "https://")):
            base = "https://" + base
        return base

    @staticmethod
    def _parse_accessory_keyword_csv(raw_keywords: str) -> list[str]:
        text = str(raw_keywords or "").strip().lower()
        if not text:
            return []
        tokens = re.split(r"[,;\n]+", text)
        parsed: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            normalized = re.sub(r"\s+", " ", token.strip())
            if len(normalized) < 3 or normalized in seen:
                continue
            parsed.append(normalized)
            seen.add(normalized)
        return parsed

    def _default_accessory_keyword_csv(self) -> str:
        return ", ".join(str(keyword).strip().lower() for keyword in ACCESSORY_SIGNAL_KEYWORDS)

    def _build_server_headers(self, token: str) -> dict:
        headers = {"Accept": "application/json"}
        token = (token or "").strip()
        if token:
            headers["x-api-token"] = token
        return headers

    def _upsert_server_listing_batch(self, items: list[dict]) -> tuple[int, int]:
        result = desktop_sync.apply_server_listing_batch(
            DB_PATH,
            items,
            source="poll_sync",
            min_seq_id_exclusive=0,
        )
        return result.inserted, result.updated

    def _refresh_after_server_sync(self, inserted: int, updated: int, since_id: int, *, status_text: str | None = None):
        now = time.time()
        if (now - self.server_sync_last_ui_refresh) >= 0.75:
            self.refresh_listings()
            self.server_sync_last_ui_refresh = now
        self.status_bar.config(text=status_text or f"Server sync active | +{inserted} new, {updated} updated | cursor={since_id}")

    def _server_sync_loop(self, stop_event: threading.Event):
        base_url = self._normalize_server_base_url(self._get_scraper_setting("server_api_base_url", ""))
        if not base_url:
            self.root.after(0, lambda: self.status_bar.config(text="Server sync disabled: API URL is empty"))
            return

        token = self._get_scraper_setting("server_api_token", "")
        headers = self._build_server_headers(token)
        poll_seconds = max(1, self._safe_int(self._get_scraper_setting("server_sync_poll_seconds", "2"), 2))
        since_id = max(0, self._safe_int(self._get_scraper_setting("server_sync_since_id", "0"), 0))
        backoff_seconds = poll_seconds

        while not stop_event.is_set():
            try:
                total_inserted = 0
                total_updated = 0

                while not stop_event.is_set():
                    response = requests.get(
                        f"{base_url}/listings",
                        params={"since_id": since_id, "limit": 500},
                        headers=headers,
                        timeout=(8, 30),
                    )
                    if response.status_code == 401:
                        raise RuntimeError("Unauthorized (APP_API_TOKEN mismatch).")
                    response.raise_for_status()

                    payload = response.json() if response.content else {}
                    items = payload.get("items") or []
                    next_since = max(since_id, self._safe_int(payload.get("next_since_id"), since_id))

                    if items:
                        inserted, updated = self._upsert_server_listing_batch(items)
                        total_inserted += inserted
                        total_updated += updated

                    if next_since != since_id:
                        since_id = next_since
                        self._set_scraper_setting("server_sync_since_id", str(since_id))

                    # Continue loop immediately while catching up backlog pages.
                    if len(items) < 500:
                        break

                if total_inserted or total_updated:
                    self.root.after(0, self._refresh_after_server_sync, total_inserted, total_updated, since_id)

                backoff_seconds = poll_seconds
                stop_event.wait(poll_seconds)
            except Exception as exc:
                backoff_seconds = min(60, max(poll_seconds, backoff_seconds * 2))
                self.root.after(
                    0,
                    lambda msg=f"Server sync retry in {backoff_seconds}s: {exc}": self.status_bar.config(text=msg),
                )
                stop_event.wait(backoff_seconds)

    def _stop_server_sync(self):
        coordinator = self.server_sync_coordinator
        self.server_sync_coordinator = None
        self.server_sync_stop_event = None
        self.server_sync_thread = None
        if self.server_sync_event_job is not None:
            with suppress(Exception):
                self.root.after_cancel(self.server_sync_event_job)
            self.server_sync_event_job = None
        while True:
            try:
                self.server_sync_event_queue.get_nowait()
            except queue.Empty:
                break
        if coordinator is not None:
            coordinator.stop()

    def _is_server_sync_enabled(self) -> bool:
        return self._is_truthy(self._get_scraper_setting("server_sync_enabled", "0"))

    def _apply_operating_mode_to_controls(self):
        """Disable local scrape actions when VPS/server sync mode is enabled."""
        server_mode = self._is_server_sync_enabled()
        if hasattr(self, "run_scraper_button"):
            self.run_scraper_button.config(state=tk.DISABLED if server_mode else tk.NORMAL)
        if hasattr(self, "cancel_scraper_button"):
            self.cancel_scraper_button.config(state=tk.DISABLED)

    def _start_or_restart_server_sync_from_settings(self):
        self._stop_server_sync()
        self._apply_operating_mode_to_controls()

        if desktop_sync.aiohttp is None:
            self.status_bar.config(text="Server sync unavailable: 'aiohttp' dependency missing")
            return

        enabled = self._is_truthy(self._get_scraper_setting("server_sync_enabled", "0"))
        if not enabled:
            return
        base_url = self._normalize_server_base_url(self._get_scraper_setting("server_api_base_url", ""))
        if not base_url:
            self.status_bar.config(text="Server sync disabled: API URL is empty")
            return

        poll_seconds = max(1, self._safe_int(self._get_scraper_setting("server_sync_poll_seconds", "2"), 2))
        since_id = max(0, self._safe_int(self._get_scraper_setting("server_sync_since_id", "0"), 0))
        config = desktop_sync.ServerSyncConfig(
            base_url=base_url,
            token=self._get_scraper_setting("server_api_token", ""),
            poll_seconds=poll_seconds,
            since_id=since_id,
        )
        self.server_sync_event_queue = queue.Queue()
        self.server_sync_coordinator = desktop_sync.ThreadedServerSyncCoordinator(
            config=config,
            db_path=DB_PATH,
            event_queue=self.server_sync_event_queue,
        )
        self.server_sync_coordinator.start()
        self.server_sync_event_job = self.root.after(150, self._drain_server_sync_events)
        self.status_bar.config(text="Server sync starting...")

    def _drain_server_sync_events(self):
        self.server_sync_event_job = None
        while True:
            try:
                event = self.server_sync_event_queue.get_nowait()
            except queue.Empty:
                break

            event_type = str(event.get("type") or "")
            if event_type == "status":
                self.status_bar.config(text=str(event.get("text") or "Server sync active"))
                continue
            if event_type == "sync_applied":
                self._refresh_after_server_sync(
                    self._safe_int(event.get("inserted"), 0),
                    self._safe_int(event.get("updated"), 0),
                    self._safe_int(event.get("since_id"), 0),
                    status_text=str(event.get("status_text") or ""),
                )

        if self.server_sync_coordinator is not None:
            self.server_sync_event_job = self.root.after(150, self._drain_server_sync_events)

    def _worker_display_name(self, worker_name: str) -> str:
        worker = str(worker_name or "").strip()
        if worker == "worker":
            return "Worker 1"
        match = re.search(r"_(\d+)$", worker)
        if match:
            return f"Worker {match.group(1)}"
        return worker or "Worker"

    @staticmethod
    def _worker_status_is_healthy(status_value: str) -> bool:
        return str(status_value or "").strip().lower() in {"ok", "running", "ready"}

    @staticmethod
    def _parse_iso_datetime(value) -> datetime | None:
        text = str(value or "").strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return None

    def _seconds_until_iso(self, value) -> int:
        target = self._parse_iso_datetime(value)
        if target is None:
            return 0
        now = datetime.now(target.tzinfo) if target.tzinfo else datetime.now()
        return max(0, int((target - now).total_seconds()))

    @staticmethod
    def _compact_proxy_host(proxy_server: str) -> str:
        raw = str(proxy_server or "").strip()
        if not raw:
            return ""
        if "://" not in raw:
            raw = f"socks5://{raw}"
        parsed = urlparse(raw)
        host = str(parsed.hostname or "").strip()
        if host:
            try:
                port = parsed.port
            except ValueError:
                # Non-standard endpoints (for example query-shard/profile locks) may
                # carry non-numeric suffixes after ":"; keep them displayable.
                port = None
            if port:
                return f"{host}:{port}"
            netloc = str(parsed.netloc or "").strip()
            if netloc:
                return netloc
            return host
        return str(proxy_server or "").strip()

    def _worker_runtime_summary(self, payload: dict) -> tuple[str, str, str]:
        lease_remaining = max(0, self._safe_int(payload.get("lease_remaining_seconds"), 0))
        cooldown_remaining = max(0, self._safe_int(payload.get("cooldown_remaining_seconds"), 0))
        lease_proxy = self._compact_proxy_host(payload.get("leased_proxy_server") or "")

        if lease_remaining > 0 and lease_proxy:
            lease_text = f"leased {lease_proxy} ({self._format_elapsed(lease_remaining)})"
        else:
            lease_text = "no active lease"

        if cooldown_remaining > 0:
            cooldown_text = f"waiting cooldown ({self._format_elapsed(cooldown_remaining)})"
        else:
            cooldown_text = "no cooldown"

        if cooldown_remaining > 0:
            strip_hint = f"cd {self._format_elapsed(cooldown_remaining)}"
        elif lease_remaining > 0:
            strip_hint = f"lease {self._format_elapsed(lease_remaining)}"
        else:
            strip_hint = "idle"

        return lease_text, cooldown_text, strip_hint

    def _set_worker_status_indicator(self, worker_name: str, healthy: bool, status_text: str):
        label = self.worker_status_labels.get(worker_name)
        canvas = self.worker_status_canvases.get(worker_name)
        dot_id = self.worker_status_dot_ids.get(worker_name)
        display_name = self._worker_display_name(worker_name)

        if label and label.winfo_exists():
            label.config(text=f"{display_name}: {status_text}")
        if canvas and canvas.winfo_exists() and dot_id:
            color = "#1faa59" if healthy else "#d93025"
            canvas.itemconfig(dot_id, fill=color, outline=color)

    def _update_worker_status_strip(self, health_by_worker: dict, error: str | None = None):
        tracked_workers = ("worker", "worker_2", "worker_3")
        if error:
            for worker_name in tracked_workers:
                self._set_worker_status_indicator(worker_name, healthy=False, status_text="offline")
            return

        for worker_name in tracked_workers:
            payload = health_by_worker.get(worker_name) or {}
            raw_status = str(payload.get("status") or "").strip().lower()
            if not raw_status:
                self._set_worker_status_indicator(worker_name, healthy=False, status_text="offline")
                continue
            healthy = self._worker_status_is_healthy(raw_status)
            _, _, strip_hint = self._worker_runtime_summary(payload)
            
            # Enhancing status info based on user request for v2.2 profiling
            route = payload.get("route_name") or ""
            route_str = f" [{route}]" if route else ""
            scraped = payload.get("listings_scraped_last_minute", 0)
            last_err = str(payload.get("last_error") or "")
            
            if raw_status == "manual_login_required":
                extra_info = "MANUAL_LOGIN_REQUIRED"
                healthy = False
            else:
                extra_info = f"{scraped}/min"
                
            status_text = f"{raw_status}{route_str} ({extra_info}) ({strip_hint})"
            
            self._set_worker_status_indicator(
                worker_name,
                healthy,
                status_text
            )

    def _fetch_vps_worker_health(self):
        ctx, error = self._get_server_api_context()
        if error:
            return {}, error

        try:
            response = requests.get(
                f"{ctx['base_url']}/worker-health",
                headers=ctx["headers"],
                timeout=(8, 25),
            )
            if response.status_code == 401:
                return {}, "Unauthorized for worker health (check Server API Token)."
            response.raise_for_status()
            payload = response.json() if response.content else {}
        except Exception as exc:
            return {}, f"Failed to fetch worker health: {exc}"

        health_by_worker = {}
        for item in (payload.get("items") or []):
            worker_name = str(item.get("worker_name") or "").strip()
            if worker_name:
                health_by_worker[worker_name] = item
        return health_by_worker, None

    def _apply_worker_status_poll_result(self, health_by_worker: dict, error: str | None):
        self.worker_status_poll_inflight = False
        if not self.worker_status_polling_active or not self.root.winfo_exists():
            return
        self._update_worker_status_strip(health_by_worker, error=error)
        self.worker_status_poll_job = self.root.after(
            int(self.worker_status_poll_interval_seconds * 1000),
            self._poll_worker_status_once,
        )

    def _poll_worker_status_once(self):
        if not self.worker_status_polling_active or not self.root.winfo_exists():
            return
        if self.worker_status_poll_inflight:
            return
        if requests is None:
            self._update_worker_status_strip({}, error="requests dependency missing")
            self.worker_status_poll_job = self.root.after(
                int(self.worker_status_poll_interval_seconds * 1000),
                self._poll_worker_status_once,
            )
            return

        self.worker_status_poll_inflight = True

        def worker():
            health_by_worker, error = self._fetch_vps_worker_health()
            try:
                if self.worker_status_polling_active:
                    self.root.after(0, lambda: self._apply_worker_status_poll_result(health_by_worker, error))
            except Exception:
                pass

        threading.Thread(
            target=worker,
            daemon=True,
            name="worker-status-poller",
        ).start()

    def _start_worker_status_polling(self):
        self._stop_worker_status_polling()
        self.worker_status_polling_active = True
        self.worker_status_poll_inflight = False
        self._poll_worker_status_once()

    def _stop_worker_status_polling(self):
        self.worker_status_polling_active = False
        job = self.worker_status_poll_job
        self.worker_status_poll_job = None
        if job:
            try:
                self.root.after_cancel(job)
            except Exception:
                pass

    def _append_server_monitor_output(self, text: str):
        if self.server_monitor_text and self.server_monitor_text.winfo_exists():
            self.server_monitor_text.config(state=tk.NORMAL)
            self.server_monitor_text.insert(tk.END, text)
            
            # Prevent Tkinter text overflow freeze on MacOS
            try:
                line_count = int(self.server_monitor_text.index('end-1c').split('.')[0])
                if line_count > 5000:
                    self.server_monitor_text.delete("1.0", f"{line_count - 5000}.0")
            except Exception:
                pass
                
            self.server_monitor_text.see(tk.END)
            self.server_monitor_text.config(state=tk.DISABLED)

    def _clear_server_monitor_output(self):
        if self.server_monitor_text and self.server_monitor_text.winfo_exists():
            self.server_monitor_text.config(state=tk.NORMAL)
            self.server_monitor_text.delete("1.0", tk.END)
            self.server_monitor_text.config(state=tk.DISABLED)

    def _server_monitor_banner(self, title: str) -> str:
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return f"\n[{ts}] {title}\n{'-' * 78}\n"

    def _get_server_ssh_context(self):
        enabled = self._is_truthy(self._get_scraper_setting("server_ssh_enabled", "1"))
        if not enabled:
            return None, "Server SSH monitoring is disabled in Scraper settings."

        user = self._get_scraper_setting("server_ssh_user", "ubuntu").strip() or "ubuntu"
        host = self._get_scraper_setting("server_ssh_host", "").strip()
        project_dir = self._get_scraper_setting("server_ssh_project_dir", "/home/ubuntu/iphone-flipper-server/server").strip()

        if not host:
            return None, "Set 'Server SSH Host' in Scraper settings first."

        return {"user": user, "host": host, "project_dir": project_dir}, None

    def _parse_server_monitor_services(self, raw_value: str) -> list[str]:
        tokens = [part.strip() for part in re.split(r"[,\s]+", str(raw_value or "").strip()) if part.strip()]
        services: list[str] = []
        seen: set[str] = set()
        for token in tokens:
            if not re.fullmatch(r"[A-Za-z0-9_.-]+", token):
                continue
            if token in seen:
                continue
            seen.add(token)
            services.append(token)

        if not services:
            services = ["worker", "worker_2", "worker_3"]
            seen = set(services)

        for required in ("worker", "worker_2", "worker_3"):
            if required not in seen:
                services.append(required)
                seen.add(required)
        return services

    def _normalize_server_monitor_services(self, raw_value: str) -> str:
        return " ".join(self._parse_server_monitor_services(raw_value))

    def _get_server_monitor_services(self) -> str:
        raw = self._get_scraper_setting(
            "server_monitor_worker_services",
            "worker worker_2 worker_3",
        )
        services = self._parse_server_monitor_services(raw)
        return " ".join(shlex.quote(svc) for svc in services)

    def _run_server_ssh_command(self, title: str, remote_command: str, timeout_seconds: int = 90):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            self.status_bar.config(text=error)
            return

        self._append_server_monitor_output(self._server_monitor_banner(title))
        self.status_bar.config(text=f"Running VPS check: {title} ...")

        ssh_target = f"{ctx['user']}@{ctx['host']}"
        ssh_cmd = ["ssh", "-o", "BatchMode=yes", ssh_target, remote_command]

        def worker():
            try:
                result = subprocess.run(
                    ssh_cmd,
                    cwd=Path(__file__).parent,
                    capture_output=True,
                    text=True,
                    timeout=timeout_seconds,
                )
                output = (result.stdout or "").strip()
                err = (result.stderr or "").strip()

                if result.returncode != 0:
                    body = (
                        (output + "\n" if output else "")
                        + (err + "\n" if err else "")
                        + f"(exit_code={result.returncode})\n"
                    )
                else:
                    body = (output + "\n") if output else "(no output)\n"
            except Exception as exc:
                body = f"Error running SSH command: {exc}\n"

            self.root.after(0, lambda: self._append_server_monitor_output(body))
            self.root.after(0, lambda: self.status_bar.config(text=f"VPS check finished: {title}"))

        threading.Thread(target=worker, daemon=True, name=f"server-monitor-{title.lower().replace(' ', '-')}").start()

    def _stop_server_log_stream(self, update_status: bool = True):
        proc = self.server_log_stream_process
        self.server_log_stream_process = None

        if proc and proc.poll() is None:
            def kill_bg():
                try:
                    proc.terminate()
                    proc.wait(timeout=3)
                except Exception:
                    try:
                        proc.kill()
                    except Exception:
                        pass
            threading.Thread(target=kill_bg, daemon=True).start()

        self.server_log_stream_thread = None
        if update_status:
            self.status_bar.config(text="Stopped live worker logs")

    def _start_server_worker_log_stream(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        # Keep a single active stream to avoid duplicated interleaved output.
        self._stop_server_log_stream(update_status=False)

        services = self._get_server_monitor_services()
        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = f"cd {project_q}/infra && docker compose --env-file ../.env logs -f --tail=5 {services}"
        ssh_target = f"{ctx['user']}@{ctx['host']}"
        ssh_cmd = ["ssh", "-o", "BatchMode=yes", "-tt", ssh_target, remote_cmd]

        self._append_server_monitor_output(self._server_monitor_banner("Worker Logs (live)"))
        self.status_bar.config(text="Streaming worker logs live...")

        log_queue = queue.Queue()

        def worker():
            proc = None
            try:
                # Use binary unbuffered reading from pipe
                proc = subprocess.Popen(
                    ssh_cmd,
                    cwd=Path(__file__).parent,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    bufsize=0,
                )
                self.server_log_stream_process = proc
                if proc.stdout is not None:
                    # Read byte chunks as they arrive to avoid any line-buffering blocks
                    for chunk in iter(lambda: proc.stdout.read(1024), b""):
                        text_chunk = chunk.decode(errors="replace")
                        log_queue.put(text_chunk)
                        
                exit_code = proc.wait(timeout=3)
                if exit_code not in (0, -15, 255):  # 255 is SSH generic exit
                    log_queue.put(f"(live log stream exited with code {code})\n")
            except Exception as exc:
                log_queue.put(f"Live log stream error: {exc}\n")
            finally:
                self.server_log_stream_process = None
                self.server_log_stream_thread = None
                self.root.after(0, lambda: self.status_bar.config(text="Worker live log stream stopped"))

        thread = threading.Thread(target=worker, daemon=True, name="server-worker-live-logs")
        self.server_log_stream_thread = thread
        thread.start()

        def pump_gui_queue():
            if not self.server_log_stream_thread and log_queue.empty():
                return
            buf = []
            max_chunks = 15  # Avoid freezing the UI tick by capping chunks string concatenation
            while not log_queue.empty() and len(buf) < max_chunks:
                try:
                    buf.append(log_queue.get_nowait())
                except queue.Empty:
                    break
            if buf:
                self._append_server_monitor_output("".join(buf))
            # Schedule the next pump
            self.root.after(100, pump_gui_queue)

        # Start pumping the logs into the UI
        self.root.after(100, pump_gui_queue)

    def _sync_accessory_filter_settings_to_vps(self, keywords_csv: str, max_price_text: str):
        ctx, error = self._get_server_ssh_context()
        if error:
            return

        project_dir = str(ctx.get("project_dir") or "").strip()
        if not project_dir:
            return

        services = self._parse_server_monitor_services(
            self._get_scraper_setting("server_monitor_worker_services", "worker worker_2 worker_3")
        )

        ssh_target = f"{ctx['user']}@{ctx['host']}"
        remote_script = f"""python3 - <<'PY'
import subprocess
from pathlib import Path

project_dir = {project_dir!r}
keywords = {keywords_csv!r}
max_price = {max_price_text!r}
services = list({services!r})
if not services:
    services = ["worker", "worker_2", "worker_3"]

infra_dir = Path(project_dir) / "infra"
compose_base = ["docker", "compose", "--env-file", "../.env"]

python_script = (
    "import os,sqlite3\\n"
    "from datetime import datetime\\n"
    "keywords=(os.getenv('ACCESSORY_KEYWORDS') or '').strip()\\n"
    "max_price=(os.getenv('ACCESSORY_MAX_PRICE') or '').strip()\\n"
    "db_path=os.getenv('IPHONE_FLIPPER_DB_PATH') or '/app/runtime/worker_1.db'\\n"
    "conn=sqlite3.connect(db_path)\\n"
    "cur=conn.cursor()\\n"
    "cur.execute('CREATE TABLE IF NOT EXISTS scraper_settings (setting_key TEXT PRIMARY KEY, setting_value TEXT, updated_at TEXT)')\\n"
    "now=datetime.now().isoformat()\\n"
    "cur.execute('INSERT INTO scraper_settings (setting_key, setting_value, updated_at) VALUES (?, ?, ?) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at', ('accessory_filter_keywords', keywords, now))\\n"
    "cur.execute('INSERT INTO scraper_settings (setting_key, setting_value, updated_at) VALUES (?, ?, ?) ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at', ('accessory_filter_max_price', max_price, now))\\n"
    "conn.commit()\\n"
    "conn.close()\\n"
    "print(f'updated {{db_path}}')\\n"
)

repair_script = (
    "set -e\\n"
    "db_path=${{IPHONE_FLIPPER_DB_PATH:-/app/runtime/worker_1.db}}\\n"
    "db_dir=$(dirname \\"$db_path\\")\\n"
    "mkdir -p \\"$db_dir\\"\\n"
    "touch \\"$db_path\\"\\n"
    "chmod 664 \\"$db_path\\" || true\\n"
    "chmod 775 \\"$db_dir\\" || true\\n"
    "if id -u pwuser >/dev/null 2>&1; then\\n"
    "  chown pwuser:pwuser \\"$db_dir\\" || true\\n"
    "  chown pwuser:pwuser \\"$db_path\\" || true\\n"
    "fi\\n"
)

def _run_update(service_name):
    cmd = compose_base + [
        "exec",
        "-T",
        "-e",
        f"ACCESSORY_KEYWORDS={{keywords}}",
        "-e",
        f"ACCESSORY_MAX_PRICE={{max_price}}",
        service_name,
        "python3",
        "-c",
        python_script,
    ]
    return subprocess.run(
        cmd,
        cwd=str(infra_dir),
        capture_output=True,
        text=True,
        timeout=90,
    )

def _run_permission_repair(service_name):
    cmd = compose_base + [
        "exec",
        "-T",
        "-u",
        "0",
        service_name,
        "sh",
        "-lc",
        repair_script,
    ]
    return subprocess.run(
        cmd,
        cwd=str(infra_dir),
        capture_output=True,
        text=True,
        timeout=90,
    )

updated = 0
failed = []
repaired = []
for service in services:
    try:
        result = _run_update(service)
    except Exception as exc:
        failed.append((service, str(exc)))
        continue
    if result.returncode == 0:
        updated += 1
        continue

    error_text = (result.stderr or result.stdout or "").strip()
    if "readonly database" in error_text.lower():
        try:
            repair_result = _run_permission_repair(service)
        except Exception as exc:
            failed.append((service, error_text + "\\npermission repair error: " + str(exc)))
            continue

        if repair_result.returncode != 0:
            repair_text = (repair_result.stderr or repair_result.stdout or "").strip()
            repair_detail = repair_text if repair_text else "exit_code=" + str(repair_result.returncode)
            failed.append((service, error_text + "\\npermission repair failed: " + repair_detail))
            continue

        try:
            retry_result = _run_update(service)
        except Exception as exc:
            failed.append((service, "retry after permission repair failed: " + str(exc)))
            continue

        if retry_result.returncode == 0:
            updated += 1
            repaired.append(service)
            continue

        retry_text = (retry_result.stderr or retry_result.stdout or "").strip()
        failed.append((service, retry_text or f"exit_code={{retry_result.returncode}}"))
    else:
        failed.append((service, error_text or f"exit_code={{result.returncode}}"))

if updated <= 0:
    print("failed to update accessory filter settings in worker containers")
    for service, err in failed:
        print(f"  {{service}}: {{err}}")
    raise SystemExit(1)

print(f"updated accessory filter settings on {{updated}} worker service(s)")
if repaired:
    print("auto-repaired worker runtime DB permissions for:")
    for service in repaired:
        print(f"  {{service}}")
if failed:
    print("partial failures:")
    for service, err in failed:
        print(f"  {{service}}: {{err}}")
PY"""

        ssh_cmd = ["ssh", "-o", "BatchMode=yes", ssh_target, remote_script]

        def worker():
            try:
                result = subprocess.run(
                    ssh_cmd,
                    cwd=Path(__file__).parent,
                    capture_output=True,
                    text=True,
                    timeout=120,
                )
                output = (result.stdout or "").strip()
                err = (result.stderr or "").strip()
                if result.returncode == 0:
                    status_text = output or "Accessory filter settings pushed to VPS worker runtime DBs"
                else:
                    status_text = (
                        "Saved locally; VPS accessory-filter sync failed: "
                        + (err or output or f"exit code {result.returncode}")
                    )
            except Exception as exc:
                status_text = f"Saved locally; VPS accessory-filter sync error: {exc}"
            self.root.after(0, lambda: self.status_bar.config(text=status_text))

        threading.Thread(
            target=worker,
            daemon=True,
            name="vps-accessory-filter-sync",
        ).start()

    def check_server_api_health(self):
        base_url = self._normalize_server_base_url(self._get_scraper_setting("server_api_base_url", ""))
        if not base_url:
            messagebox.showwarning("Server Monitor", "Set 'Server API Base URL' in Scraper settings first.")
            return

        headers = self._build_server_headers(self._get_scraper_setting("server_api_token", ""))
        self._append_server_monitor_output(self._server_monitor_banner("API Health"))
        self.status_bar.config(text="Checking VPS API health ...")

        def worker():
            try:
                response = requests.get(f"{base_url}/healthz", headers=headers, timeout=(8, 25))
                text = response.text.strip()
                body = f"HTTP {response.status_code}\n{text}\n"
            except Exception as exc:
                body = f"Health check failed: {exc}\n"
            self.root.after(0, lambda: self._append_server_monitor_output(body))
            self.root.after(0, lambda: self.status_bar.config(text="VPS API health check finished"))

        threading.Thread(target=worker, daemon=True, name="server-health-check").start()

    def check_server_worker_status(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        services = self._get_server_monitor_services()
        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = f"cd {project_q}/infra && docker compose --env-file ../.env ps {services}"
        self._run_server_ssh_command("Worker Status", remote_cmd)

    def start_server_scrapers(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        services = self._get_server_monitor_services()
        if not messagebox.askyesno(
            "Start VPS Scrapers",
            "Start scraper worker services on VPS now?",
            parent=self.settings_window or self.root,
        ):
            return

        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = (
            f"cd {project_q}/infra && "
            f"docker compose --env-file ../.env up -d {services} && "
            f"docker compose --env-file ../.env ps {services}"
        )
        if not (self.server_monitor_window and self.server_monitor_window.winfo_exists()):
            self.show_server_activity_monitor()
        self._run_server_ssh_command("Start Scrapers", remote_cmd, timeout_seconds=120)
        self.status_bar.config(text="Starting VPS scraper services ...")

    def stop_server_scrapers(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        services = self._get_server_monitor_services()
        if not messagebox.askyesno(
            "Stop VPS Scrapers",
            "Stop scraper worker services on VPS now?",
            parent=self.settings_window or self.root,
        ):
            return

        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = (
            f"cd {project_q}/infra && "
            f"docker compose --env-file ../.env stop {services} && "
            f"docker compose --env-file ../.env ps {services}"
        )
        if not (self.server_monitor_window and self.server_monitor_window.winfo_exists()):
            self.show_server_activity_monitor()
        self._run_server_ssh_command("Stop Scrapers", remote_cmd, timeout_seconds=120)
        self.status_bar.config(text="Stopping VPS scraper services ...")

    def check_server_worker_logs(self):
        self._start_server_worker_log_stream()

    def check_server_listing_count(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = (
            f"cd {project_q}/infra && "
            "docker compose --env-file ../.env exec -T postgres "
            "sh -lc 'psql -U \"$POSTGRES_USER\" -d \"$POSTGRES_DB\" -c "
            "\"select count(*) as listings_count from listings;\"'"
        )
        self._run_server_ssh_command("Listing Count", remote_cmd)

    def _refresh_query_manager_routes(self):
        if not self.query_manager_tree:
            return

        selected_key = None
        selection = self.query_manager_tree.selection()
        if selection:
            selected_key = selection[0]

        for item in self.query_manager_tree.get_children():
            self.query_manager_tree.delete(item)
        self.query_manager_routes_by_key = {}

        routes, health_by_worker, error = self._fetch_vps_scraper_payloads()
        if error:
            self.status_bar.config(text=error)
            if self.query_manager_window and self.query_manager_window.winfo_exists():
                messagebox.showwarning("Route Queries", error)
            return

        health_by_route_name: dict[str, dict] = {}
        for worker_name, health in (health_by_worker or {}).items():
            route_name = str((health or {}).get("route_name") or "").strip()
            if route_name and route_name not in health_by_route_name:
                health_by_route_name[route_name] = dict(health)
                health_by_route_name[route_name]["_active_worker_name"] = worker_name

        matched_selected = None
        for route in sorted(
            routes,
            key=lambda item: (
                str(item.get("legacy_worker_name") or item.get("worker_name") or "").strip(),
                self._safe_int(item.get("priority"), 100),
                str(item.get("route_name") or "").strip(),
            ),
        ):
            route_name = str(route.get("route_name") or "").strip()
            if not route_name:
                continue
            legacy_worker_name = str(route.get("legacy_worker_name") or route.get("worker_name") or "").strip() or "central"
            health = health_by_route_name.get(route_name, {})
            status = str(health.get("status") or route.get("status") or "unknown").strip() or "unknown"
            lane = str(route.get("effective_lane") or route.get("computed_lane") or "warm").strip() or "warm"
            key = f"route::{route_name}"
            query_csv = str(route.get("search_queries") or "BUCKETS").strip() or "BUCKETS"

            self.query_manager_tree.insert(
                "",
                tk.END,
                iid=key,
                values=(
                    legacy_worker_name,
                    route_name,
                    status,
                    lane,
                    query_csv,
                ),
            )
            self.query_manager_routes_by_key[key] = {
                "route_name": route_name,
                "legacy_worker_name": legacy_worker_name,
                "active_worker_name": str(health.get("_active_worker_name") or "").strip() or None,
                "route": route,
                "search_queries": query_csv,
            }
            if selected_key == key:
                matched_selected = key

        if matched_selected and matched_selected in self.query_manager_routes_by_key:
            self.query_manager_tree.selection_set(matched_selected)
            self.query_manager_tree.focus(matched_selected)
            self.query_manager_tree.see(matched_selected)
            self._on_query_manager_route_selected()
        elif self.query_manager_tree.get_children():
            first = self.query_manager_tree.get_children()[0]
            self.query_manager_tree.selection_set(first)
            self.query_manager_tree.focus(first)
            self._on_query_manager_route_selected()

    def _on_query_manager_route_selected(self, event=None):
        if not self.query_manager_tree or not self.query_manager_query_var:
            return
        selection = self.query_manager_tree.selection()
        if not selection:
            self.query_manager_query_var.set("")
            return
        key = selection[0]
        data = self.query_manager_routes_by_key.get(key, {})
        self.query_manager_query_var.set(str(data.get("search_queries") or "").strip())

    def _save_query_manager_route_queries(self):
        if not self.query_manager_tree:
            return
        selection = self.query_manager_tree.selection()
        if not selection:
            messagebox.showwarning("Route Queries", "Select a central route first.")
            return
        key = selection[0]
        data = self.query_manager_routes_by_key.get(key)
        if not data:
            messagebox.showwarning("Route Queries", "Selected route is no longer available. Refresh and retry.")
            return

        route_name = str(data.get("route_name") or "").strip()
        if not route_name:
            messagebox.showwarning("Route Queries", "Invalid route selection.")
            return

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("Route Queries", error)
            return

        query_csv = (self.query_manager_query_var.get() if self.query_manager_query_var else "").strip()
        query_items = [token.strip() for token in query_csv.split(",") if token.strip()]
        try:
            response = requests.put(
                f"{ctx['base_url']}/routes/{route_name}/queries",
                headers=ctx["headers"],
                json={"queries": query_items},
                timeout=(8, 30),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
        except Exception as exc:
            messagebox.showerror("Save Failed", f"Failed to save route queries:\n{exc}")
            self.status_bar.config(text=f"Route query save failed for {route_name}")
            return

        self.status_bar.config(text=f"Saved canonical query set for route {route_name}")
        self._refresh_query_manager_routes()

    def _append_query_to_selected_route(self):
        if not self.query_manager_query_var:
            return

        query = simpledialog.askstring(
            "Add Query",
            "Query text to add for selected route:",
            parent=self.query_manager_window or self.root,
        )
        if query is None:
            return
        query = query.strip()
        if not query:
            return

        existing = [
            token.strip()
            for token in str(self.query_manager_query_var.get() or "").split(",")
            if token.strip()
        ]
        lowered = {token.lower() for token in existing}
        if query.lower() not in lowered:
            existing.append(query)
        self.query_manager_query_var.set(", ".join(existing))

    def _save_query_manager_negative_keywords(self):
        if not self.query_manager_negative_keywords_var:
            return

        raw_keywords = self.query_manager_negative_keywords_var.get().strip()
        keywords = self._parse_accessory_keyword_csv(raw_keywords)
        if not keywords:
            keywords = self._parse_accessory_keyword_csv(self._default_accessory_keyword_csv())
        keywords_csv = ", ".join(keywords)

        max_price_raw = (
            self.query_manager_accessory_max_price_var.get().strip()
            if self.query_manager_accessory_max_price_var
            else self._get_scraper_setting("accessory_filter_max_price", "120")
        )
        accessory_max_price = self._safe_float(max_price_raw or "120")
        if accessory_max_price is None or accessory_max_price <= 0:
            messagebox.showwarning("Validation Error", "Accessory max price must be a number greater than 0.")
            return
        accessory_max_price_text = f"{float(accessory_max_price):.2f}".rstrip("0").rstrip(".")

        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for key, value in (
            ("accessory_filter_keywords", keywords_csv),
            ("accessory_filter_max_price", accessory_max_price_text),
        ):
            cursor.execute(
                """
                INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """,
                (key, value, now_iso),
            )
        conn.commit()
        conn.close()

        self.query_manager_negative_keywords_var.set(keywords_csv)
        if self.query_manager_accessory_max_price_var:
            self.query_manager_accessory_max_price_var.set(accessory_max_price_text)

        # Keep settings-window vars in sync when opened.
        if isinstance(self.vps_scraper_form_vars, dict):
            if "accessory_filter_keywords" in self.vps_scraper_form_vars:
                self.vps_scraper_form_vars["accessory_filter_keywords"].set(keywords_csv)
            if "accessory_filter_max_price" in self.vps_scraper_form_vars:
                self.vps_scraper_form_vars["accessory_filter_max_price"].set(accessory_max_price_text)

        self._sync_accessory_filter_settings_to_vps(keywords_csv, accessory_max_price_text)
        self.status_bar.config(text="Saved universal negative keywords")
        messagebox.showinfo("Saved", "Negative keywords saved and synced to VPS worker runtime DBs.")

    def show_query_keyword_manager(self):
        """Open central-route query manager and universal negative-keyword controls."""
        if self.query_manager_window and self.query_manager_window.winfo_exists():
            self.query_manager_window.lift()
            self.query_manager_window.focus_force()
            self._refresh_query_manager_routes()
            return

        self.query_manager_query_var = tk.StringVar(value="")
        self.query_manager_negative_keywords_var = tk.StringVar(
            value=self._get_scraper_setting("accessory_filter_keywords", self._default_accessory_keyword_csv())
        )
        self.query_manager_accessory_max_price_var = tk.StringVar(
            value=self._get_scraper_setting("accessory_filter_max_price", "120")
        )

        window = tk.Toplevel(self.root)
        window.title("Central Route Queries & Negative Keywords")
        window.geometry("1240x760")
        self.query_manager_window = window

        def on_close():
            self.query_manager_window = None
            self.query_manager_tree = None
            self.query_manager_routes_by_key = {}
            self.query_manager_query_var = None
            self.query_manager_negative_keywords_var = None
            self.query_manager_accessory_max_price_var = None
            window.destroy()

        window.protocol("WM_DELETE_WINDOW", on_close)

        main = ttk.Frame(window)
        main.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        main.columnconfigure(0, weight=3)
        main.columnconfigure(1, weight=2)
        main.rowconfigure(0, weight=1)

        routes_frame = ttk.LabelFrame(main, text="Central Route Query Sets")
        routes_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        routes_frame.columnconfigure(0, weight=1)
        routes_frame.rowconfigure(0, weight=1)

        self.query_manager_tree = ttk.Treeview(
            routes_frame,
            columns=("Legacy Worker", "Route", "Status", "Lane", "Queries"),
            show="headings",
            selectmode="browse",
        )
        for col, width in [
            ("Legacy Worker", 130),
            ("Route", 190),
            ("Status", 90),
            ("Lane", 90),
            ("Queries", 430),
        ]:
            self.query_manager_tree.heading(col, text=col)
            self.query_manager_tree.column(col, width=width, anchor=tk.W)
        route_vsb = ttk.Scrollbar(routes_frame, orient="vertical", command=self.query_manager_tree.yview)
        self.query_manager_tree.configure(yscrollcommand=route_vsb.set)
        self.query_manager_tree.grid(row=0, column=0, sticky="nsew", padx=8, pady=8)
        route_vsb.grid(row=0, column=1, sticky="ns", pady=8)
        self.query_manager_tree.bind("<<TreeviewSelect>>", self._on_query_manager_route_selected)

        route_editor = ttk.Frame(routes_frame)
        route_editor.grid(row=1, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 10))
        route_editor.columnconfigure(1, weight=1)
        ttk.Label(route_editor, text="Selected Route Query CSV:").grid(row=0, column=0, sticky="w")
        ttk.Entry(route_editor, textvariable=self.query_manager_query_var).grid(
            row=0, column=1, sticky="ew", padx=(8, 0)
        )

        route_actions = ttk.Frame(routes_frame)
        route_actions.grid(row=2, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 10))
        route_actions.columnconfigure(0, weight=1)
        route_actions.columnconfigure(1, weight=1)
        route_actions.columnconfigure(2, weight=1)
        route_actions.columnconfigure(3, weight=1)
        ttk.Button(route_actions, text="Save Route Queries", command=self._save_query_manager_route_queries).grid(
            row=0, column=0, sticky="ew", padx=4
        )
        ttk.Button(route_actions, text="Add Query", command=self._append_query_to_selected_route).grid(
            row=0, column=1, sticky="ew", padx=4
        )
        ttk.Button(route_actions, text="Clear Query Set", command=lambda: self.query_manager_query_var.set("")).grid(
            row=0, column=2, sticky="ew", padx=4
        )
        ttk.Button(route_actions, text="Refresh Routes", command=self._refresh_query_manager_routes).grid(
            row=0, column=3, sticky="ew", padx=4
        )

        ttk.Label(
            routes_frame,
            text=(
                "Worker_3 is intended for fast-lane newest-listing sweeps (default query: iPhone). "
                "This panel edits the canonical query set for each central route. "
                "Workers remain generic executors and profiles are managed separately."
            ),
            wraplength=760,
        ).grid(row=3, column=0, columnspan=2, sticky="w", padx=8, pady=(0, 8))

        keywords_frame = ttk.LabelFrame(main, text="Universal Negative Keywords (Accessory Filter)")
        keywords_frame.grid(row=0, column=1, sticky="nsew")
        keywords_frame.columnconfigure(0, weight=1)
        keywords_frame.columnconfigure(1, weight=1)

        ttk.Label(
            keywords_frame,
            text="These keywords are applied globally to suppress accessory-only listings (case/cover/charger/etc).",
            wraplength=380,
        ).grid(row=0, column=0, columnspan=2, sticky="w", padx=8, pady=(8, 4))

        ttk.Label(keywords_frame, text="Negative Keywords (CSV):").grid(row=1, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(keywords_frame, textvariable=self.query_manager_negative_keywords_var).grid(
            row=2, column=0, columnspan=2, sticky="ew", padx=8, pady=(0, 8)
        )

        ttk.Label(keywords_frame, text="Accessory Max Price:").grid(row=3, column=0, sticky="w", padx=8, pady=6)
        ttk.Entry(
            keywords_frame,
            textvariable=self.query_manager_accessory_max_price_var,
            width=16,
        ).grid(row=3, column=1, sticky="w", padx=8, pady=6)

        ttk.Button(
            keywords_frame,
            text="Save Negative Keywords",
            command=self._save_query_manager_negative_keywords,
        ).grid(row=4, column=0, columnspan=2, sticky="ew", padx=8, pady=(8, 10))

        self._refresh_query_manager_routes()

    def send_server_telegram_test(self):
        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("Server Monitor", error)
            return

        project_q = shlex.quote(ctx["project_dir"])
        remote_cmd = (
            "bash -lc "
            + shlex.quote(
                f"""
                set -e
                cd {project_q}/infra
                SERVICE=$(docker compose --env-file ../.env ps --services --filter status=running | grep -E '^worker(_2)?$' | head -n 1 || true)
                if [ -z "$SERVICE" ]; then
                  SERVICE=worker
                fi
                docker compose --env-file ../.env exec -T "$SERVICE" python3 - <<'PY'
import socket
from datetime import datetime, timezone
import os

import requests

token = (os.getenv("TELEGRAM_BOT_TOKEN") or "").strip()
chat_id = (os.getenv("TELEGRAM_CHAT_ID") or "").strip()
if not token or not chat_id:
    raise SystemExit("Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in server/.env")

message = (
    "iPhone Flipper test notification\\n"
    f"Time: {{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}}\\n"
    f"Host: {{socket.gethostname()}}\\n"
    "Source: VPS worker"
)
url = f"https://api.telegram.org/bot{{token}}/sendMessage"
resp = requests.post(
    url,
    json={{
        "chat_id": chat_id,
        "text": message,
        "disable_web_page_preview": True,
    }},
    timeout=20,
)
print(resp.text.strip() or "<empty response>")
resp.raise_for_status()
payload = resp.json()
if not payload.get("ok"):
    raise SystemExit("Telegram API returned ok=false")
print("Telegram test sent successfully.")
PY
                """
            )
        )
        if not (self.server_monitor_window and self.server_monitor_window.winfo_exists()):
            self.show_server_activity_monitor()
        self._run_server_ssh_command("Telegram Test", remote_cmd, timeout_seconds=75)
        self.status_bar.config(text="Running VPS Telegram test...")

    def _get_server_api_context(self):
        if requests is None:
            return None, "Server API features require the 'requests' package."

        base_url = self._normalize_server_base_url(self._get_scraper_setting("server_api_base_url", ""))
        if not base_url:
            return None, "Set 'Server API Base URL' in Scraper settings first."

        token = self._get_scraper_setting("server_api_token", "")
        headers = self._build_server_headers(token)
        headers["Content-Type"] = "application/json"
        return {"base_url": base_url, "headers": headers}, None

    def _show_vps_route_warnings(self, warnings, title: str = "VPS Scrapers"):
        warning_items = [str(item).strip() for item in (warnings or []) if str(item).strip()]
        if not warning_items:
            return
        messagebox.showwarning(
            title,
            "Route configuration warning(s):\n" + "\n".join(f"- {item}" for item in warning_items[:6]),
        )

    def _workers_missing_saved_routes(self, routes, health_by_worker):
        workers_with_routes = {
            str(route.get("worker_name") or "").strip()
            for route in (routes or [])
            if str(route.get("worker_name") or "").strip()
        }
        missing_workers = []
        for worker_name, health in (health_by_worker or {}).items():
            worker_name_clean = str(worker_name or "").strip()
            if not worker_name_clean or worker_name_clean in workers_with_routes:
                continue
            route_source = str(health.get("route_source") or "env").strip().lower() or "env"
            profile_dir = str(health.get("route_user_data_dir") or "").strip()
            search_queries = str(health.get("route_search_queries") or "").strip()
            if route_source not in {"env", "db"}:
                continue
            if not profile_dir and not search_queries:
                continue
            missing_workers.append(worker_name_clean)
        return sorted(set(missing_workers))

    def _bootstrap_vps_routes_from_health(self, ctx):
        try:
            response = requests.post(
                f"{ctx['base_url']}/worker-routes/bootstrap-from-health",
                headers=ctx["headers"],
                timeout=(8, 30),
            )
            if response.status_code == 401:
                return None, "Unauthorized for worker route auto-bootstrap (check Server API Token)."
            response.raise_for_status()
            return (response.json() if response.content else {}), None
        except Exception as exc:
            return None, f"Failed to auto-bootstrap worker routes: {exc}"

    def _fetch_vps_scraper_payloads(self):
        ctx, error = self._get_server_api_context()
        if error:
            return [], {}, error

        def _fetch_routes():
            routes_resp = requests.get(
                f"{ctx['base_url']}/routes",
                headers=ctx["headers"],
                timeout=(8, 25),
            )
            if routes_resp.status_code == 401:
                raise RuntimeError("Unauthorized for central routes (check Server API Token).")
            routes_resp.raise_for_status()
            routes_payload = routes_resp.json() if routes_resp.content else {}
            return routes_payload.get("items") or []

        def _fetch_health():
            fetched_health_by_worker = {}
            health_resp = requests.get(
                f"{ctx['base_url']}/worker-health",
                headers=ctx["headers"],
                timeout=(8, 25),
            )
            if health_resp.status_code == 401:
                raise RuntimeError("Unauthorized for worker health (check Server API Token).")
            health_resp.raise_for_status()
            health_payload = health_resp.json() if health_resp.content else {}
            for item in (health_payload.get("items") or []):
                worker_name = str(item.get("worker_name") or "").strip()
                if worker_name:
                    fetched_health_by_worker[worker_name] = item
            return fetched_health_by_worker

        try:
            routes = _fetch_routes()
        except Exception as exc:
            return [], {}, f"Failed to fetch central routes: {exc}"

        try:
            health_by_worker = _fetch_health()
        except Exception:
            # Routes are still useful even if health endpoint is unavailable.
            health_by_worker = {}

        return routes, health_by_worker, None

    def _load_local_proxy_profiles(self):
        profiles = []
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT COALESCE(name, ''), COALESCE(proxy_type, ''), COALESCE(host, ''), port,
                       COALESCE(username, ''), COALESCE(password, ''), COALESCE(status, 'active')
                FROM proxies
                ORDER BY id ASC
                """
            )
            rows = cursor.fetchall()
            conn.close()
        except Exception:
            return profiles

        for row in rows:
            proxy_name = str(row[0] or "").strip()
            proxy_type = str(row[1] or "").strip().lower() or "socks5"
            host = str(row[2] or "").strip()
            port = row[3]
            username = str(row[4] or "").strip()
            password = str(row[5] or "").strip()
            status = str(row[6] or "active").strip().lower()
            if not host or not port or status != "active":
                continue
            try:
                port_value = int(port)
            except (TypeError, ValueError):
                continue
            if port_value <= 0:
                continue
            profiles.append(
                {
                    "label": f"LocalProxy/{proxy_name or host}:{port_value} -> {proxy_type}://{host}:{port_value}",
                    "proxy_server": f"{proxy_type}://{host}:{port_value}",
                    "proxy_username": username or None,
                    "proxy_password": password or None,
                }
            )
        return profiles

    def _build_vps_proxy_profile_options(self, routes):
        options = {
            VPS_PROXY_PROFILE_DIRECT: {
                "proxy_server": "",
                "proxy_username": "",
                "proxy_password": "",
            },
            VPS_PROXY_PROFILE_CUSTOM: None,
        }
        for route in routes:
            worker_name = str(route.get("worker_name") or "").strip()
            route_name = str(route.get("route_name") or "").strip()
            proxy_server = str(route.get("proxy_server") or "").strip()
            if not worker_name or not route_name or not proxy_server:
                continue
            label = f"{worker_name}/{route_name} -> {proxy_server}"
            options[label] = route
        for profile in self._load_local_proxy_profiles():
            label = profile["label"]
            options[label] = {
                "proxy_server": profile["proxy_server"],
                "proxy_username": profile["proxy_username"],
                "proxy_password": profile["proxy_password"],
            }
        self.vps_proxy_profile_options = options
        return list(options.keys())

    def _existing_worker_names(self):
        names = set()
        for key in self.vps_scraper_records_by_key.keys():
            if "::" not in key:
                continue
            worker_name = key.split("::", 1)[0].strip()
            if worker_name:
                names.add(worker_name)
        return names

    def _route_counts_by_worker(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for key in self.vps_scraper_records_by_key.keys():
            if "::" not in key:
                continue
            worker_name = key.split("::", 1)[0].strip()
            if not worker_name:
                continue
            counts[worker_name] = counts.get(worker_name, 0) + 1
        return counts

    def _pick_least_loaded_worker(self, worker_names: list[str]) -> str:
        counts = self._route_counts_by_worker()
        candidates = [name.strip() for name in worker_names if str(name).strip()]
        if not candidates:
            return "worker"
        return min(candidates, key=lambda name: (counts.get(name, 0), name))

    def _suggest_next_worker_name(self):
        names = self._existing_worker_names()
        preferred = [name for name in ("worker", "worker_2", "worker_3") if name in names]
        if preferred:
            return self._pick_least_loaded_worker(preferred)
        if names:
            return self._pick_least_loaded_worker(sorted(names))
        return "worker"

    def _suggest_profile_dir_for_worker(self, worker_name: str):
        used_numbers = set()
        for data in self.vps_scraper_records_by_key.values():
            route = data.get("route") or {}
            profile = str(route.get("user_data_dir") or "").strip()
            match = re.search(r"/browser_profile_(\d+)$", profile)
            if match:
                try:
                    used_numbers.add(int(match.group(1)))
                except ValueError:
                    pass

        candidate = None
        worker_raw = (worker_name or "").strip()
        if worker_raw == "worker":
            candidate = 1
        else:
            worker_match = re.search(r"_(\d+)$", worker_raw)
            if worker_match:
                try:
                    candidate = int(worker_match.group(1))
                except ValueError:
                    candidate = None

        if candidate is None or candidate <= 0 or candidate in used_numbers:
            candidate = 1
            while candidate in used_numbers:
                candidate += 1
        return f"/app/runtime/browser_profile_{candidate}"

    @staticmethod
    def _profile_display_name(profile_dir: str) -> str:
        profile_text = str(profile_dir or "").strip()
        if not profile_text:
            return "-"
        name = Path(profile_text).name.strip()
        return name or profile_text

    def _lane_mix_display(self, worker_routes: list[dict]) -> str:
        if not worker_routes:
            return "env-only"
        counts = {"hot": 0, "warm": 0, "sweep": 0}
        for route in worker_routes:
            lane = str(route.get("effective_lane") or route.get("computed_lane") or "warm").strip().lower() or "warm"
            if lane not in counts:
                lane = "warm"
            counts[lane] += 1
        parts = [f"{counts['hot']} hot", f"{counts['warm']} warm", f"{counts['sweep']} sweep"]
        return " | ".join(parts)

    def _existing_route_names_for_worker(self, worker_name: str) -> set[str]:
        names = set()
        target = (worker_name or "").strip()
        if not target:
            return names
        for key in self.vps_scraper_records_by_key.keys():
            if "::" not in key:
                continue
            key_worker, key_route = key.split("::", 1)
            if key_worker.strip() == target and key_route.strip():
                names.add(key_route.strip())
        return names

    def _suggest_route_name_for_worker(self, worker_name: str, profile_dir: str) -> str:
        profile = (profile_dir or "").strip()
        match = re.search(r"/browser_profile_(\d+)$", profile)
        if match:
            try:
                base = f"profile_{int(match.group(1))}"
            except ValueError:
                base = "profile"
        else:
            base = "profile"

        existing = self._existing_route_names_for_worker(worker_name)
        if base not in existing:
            return base

        suffix = 2
        while True:
            candidate = f"{base}_{suffix}"
            if candidate not in existing:
                return candidate
            suffix += 1

    def _running_worker_names_from_health(self) -> list[str]:
        _, health_by_worker, error = self._fetch_vps_scraper_payloads()
        if error:
            return []
        names = []
        for worker_name, health in health_by_worker.items():
            status = str((health or {}).get("status") or "").strip().lower()
            if worker_name and status:
                names.append(worker_name)
        return sorted(set(names))

    def _normalize_worker_name_for_rotation(self, worker_name: str) -> tuple[str, bool]:
        requested = (worker_name or "").strip()
        if not requested:
            return requested, False

        running_workers = self._running_worker_names_from_health()
        if not running_workers:
            return requested, False
        if requested in running_workers:
            return requested, False

        replacement = self._pick_least_loaded_worker(running_workers)
        return replacement, True

    def _on_vps_worker_name_focus_out(self, event=None):
        worker_name_var = self.vps_scraper_form_vars.get("worker_name")
        worker_name = worker_name_var.get().strip() if worker_name_var is not None else ""
        if not worker_name:
            return
        current_profile_var = self.vps_scraper_form_vars.get("user_data_dir")
        current_profile = current_profile_var.get().strip() if current_profile_var is not None else ""
        if current_profile:
            return
        self.vps_scraper_form_vars["user_data_dir"].set(self._suggest_profile_dir_for_worker(worker_name))

    def _apply_selected_proxy_profile(self):
        selected_var = self.vps_scraper_form_vars.get("proxy_profile")
        selected_label = selected_var.get().strip() if selected_var is not None else ""
        if not selected_label:
            return
        route = self.vps_proxy_profile_options.get(selected_label)
        if not route:
            return
        self.vps_scraper_form_vars["proxy_server"].set(str(route.get("proxy_server") or ""))
        self.vps_scraper_form_vars["proxy_username"].set(str(route.get("proxy_username") or ""))
        self.vps_scraper_form_vars["proxy_password"].set(str(route.get("proxy_password") or ""))

    def _on_vps_proxy_profile_selected(self, event=None):
        self._apply_selected_proxy_profile()

    def _selected_vps_route_identity(self) -> tuple[str | None, str | None, str | None]:
        key = self.selected_vps_scraper_key
        if not key and self.vps_scraper_tree:
            selection = self.vps_scraper_tree.selection()
            if selection:
                key = selection[0]
        if not key:
            return None, None, None

        data = self.vps_scraper_records_by_key.get(key) or {}
        route = data.get("route") or {}
        worker_name = str(route.get("legacy_worker_name") or route.get("worker_name") or "").strip()
        route_name = str(route.get("route_name") or "").strip()
        if worker_name and route_name:
            return key, worker_name, route_name

        if route_name:
            return key, worker_name or None, route_name

        parts = str(key).split("::")
        if len(parts) >= 2:
            return key, parts[0].strip(), parts[1].strip()
        return key, None, None

    def _selected_vps_summary_worker(self) -> str | None:
        if not self.vps_worker_summary_tree:
            return None
        selection = self.vps_worker_summary_tree.selection()
        if not selection:
            return None
        worker_name = str(selection[0] or "").strip()
        return worker_name or None

    def _refresh_vps_worker_summary_tree(
        self,
        *,
        routes: list[dict],
        health_by_worker: dict[str, dict],
    ) -> None:
        if not self.vps_worker_summary_tree:
            return

        selected_worker_name = self._selected_vps_summary_worker()
        for item in self.vps_worker_summary_tree.get_children():
            self.vps_worker_summary_tree.delete(item)
        self.vps_worker_summary_records_by_key = {}

        grouped_routes: dict[str, list[dict]] = {}
        for route in routes:
            worker_name = str(route.get("legacy_worker_name") or route.get("worker_name") or "").strip()
            route_name = str(route.get("route_name") or "").strip()
            if not worker_name or not route_name:
                continue
            grouped_routes.setdefault(worker_name, []).append(route)

        worker_names = sorted(set(grouped_routes) | set(health_by_worker))
        matched_selected = None
        for worker_name in worker_names:
            worker_routes = grouped_routes.get(worker_name, [])
            health = health_by_worker.get(worker_name) or {}
            status = str(health.get("status") or ("configured" if worker_routes else "unknown")).strip() or "unknown"
            active_route_name = str(health.get("route_name") or "").strip()
            active_route = None
            if active_route_name:
                for route in worker_routes:
                    if str(route.get("route_name") or "").strip() == active_route_name:
                        active_route = route
                        break
            if active_route is None and worker_routes:
                active_route = sorted(
                    worker_routes,
                    key=lambda route: (
                        0 if bool(route.get("is_enabled", True)) else 1,
                        self._safe_int(route.get("priority"), 100),
                        str(route.get("route_name") or ""),
                    ),
                )[0]
            if active_route is not None and not active_route_name:
                active_route_name = str(active_route.get("route_name") or "").strip()

            active_lane = "env"
            if active_route is not None:
                active_lane = str(active_route.get("effective_lane") or active_route.get("computed_lane") or "warm").strip() or "warm"
            enabled_count = sum(1 for route in worker_routes if bool(route.get("is_enabled", True)))
            route_count_display = f"{enabled_count}/{len(worker_routes)}"
            last_run = str(
                health.get("last_run_finished_at")
                or health.get("last_run_started_at")
                or health.get("updated_at")
                or ""
            )
            last_run_display = last_run.replace("T", " ")[:19] if last_run else ""
            values = (
                worker_name,
                active_route_name or "env-backed",
                active_lane,
                route_count_display,
                self._lane_mix_display(worker_routes),
                status,
                str(max(0, self._safe_int(health.get("listings_scraped_last_minute"), 0))),
                last_run_display,
            )
            self.vps_worker_summary_tree.insert("", tk.END, iid=worker_name, values=values)
            self.vps_worker_summary_records_by_key[worker_name] = {
                "worker_name": worker_name,
                "active_route_name": active_route_name,
                "health": health,
                "routes": worker_routes,
            }
            if selected_worker_name == worker_name:
                matched_selected = worker_name

        if matched_selected and matched_selected in self.vps_worker_summary_tree.get_children():
            self.vps_worker_summary_tree.selection_set(matched_selected)
            self.vps_worker_summary_tree.focus(matched_selected)
            self.vps_worker_summary_tree.see(matched_selected)

    def _on_vps_worker_summary_selected(self, event=None):
        if not self.vps_worker_summary_tree or not self.vps_scraper_tree:
            return
        selection = self.vps_worker_summary_tree.selection()
        if not selection:
            return
        worker_name = str(selection[0] or "").strip()
        if not worker_name:
            return

        summary = self.vps_worker_summary_records_by_key.get(worker_name) or {}
        active_route_name = str(summary.get("active_route_name") or "").strip()
        candidate_key = None
        for key, data in self.vps_scraper_records_by_key.items():
            route = data.get("route") or {}
            route_worker_name = str(route.get("legacy_worker_name") or route.get("worker_name") or "").strip()
            if route_worker_name != worker_name:
                continue
            route_name = str(route.get("route_name") or "").strip()
            if active_route_name and route_name == active_route_name:
                candidate_key = key
                break
            if candidate_key is None:
                candidate_key = key

        if candidate_key and candidate_key in self.vps_scraper_tree.get_children():
            self.vps_scraper_tree.selection_set(candidate_key)
            self.vps_scraper_tree.focus(candidate_key)
            self.vps_scraper_tree.see(candidate_key)
            self._on_vps_scraper_selected()

    def _insert_vps_health_only_row(
        self,
        *,
        worker_name: str,
        health: dict,
        selected_worker_name: str | None,
        selected_route_name: str | None,
        matched_selected_key: str | None,
    ) -> str | None:
        if not self.vps_scraper_tree:
            return matched_selected_key

        worker_name_clean = str(worker_name or "").strip()
        if not worker_name_clean:
            return matched_selected_key

        route_name = str(health.get("route_name") or "").strip()
        route_key_name = route_name or "env_default"
        status = str(health.get("status") or "unknown").strip() or "unknown"
        last_run = str(
            health.get("last_run_finished_at")
            or health.get("last_run_started_at")
            or health.get("updated_at")
            or ""
        )
        last_run_display = last_run.replace("T", " ")[:19] if last_run else ""
        route_user_data_dir = str(health.get("route_user_data_dir") or "").strip()
        values = (
            worker_name_clean,
            route_name or "env_default",
            self._profile_display_name(route_user_data_dir) if route_user_data_dir else "env-backed",
            "env",
            "-",
            status,
            str(max(0, self._safe_int(health.get("listings_scraped_last_minute"), 0))),
            last_run_display,
        )
        key = f"{worker_name_clean}::__health__::{route_key_name}"
        duplicate_suffix = 2
        while key in self.vps_scraper_records_by_key:
            key = f"{worker_name_clean}::__health__::{route_key_name}::{duplicate_suffix}"
            duplicate_suffix += 1

        self.vps_scraper_tree.insert("", tk.END, iid=key, values=values)
        self.vps_scraper_records_by_key[key] = {
            "route": {
                "worker_name": worker_name_clean,
                "route_name": route_name,
                "is_enabled": True,
                "is_synthetic_health_row": True,
                "source": str(health.get("route_source") or "env").strip() or "env",
                "user_data_dir": route_user_data_dir,
                "lane_override": "",
                "computed_lane": "",
                "effective_lane": "",
                "priority_score": "",
                "priority_score_updated_at": "",
                "profitable_hit_rate": "",
                "recent_duplicate_ratio": "",
                "search_queries": str(health.get("route_search_queries") or ""),
                "proxy_mode": str(health.get("route_proxy_mode") or "fixed").strip() or "fixed",
                "proxy_server": str(health.get("route_proxy_server") or ""),
                "proxy_username": str(health.get("route_proxy_username") or ""),
                "proxy_password": str(health.get("route_proxy_password") or ""),
                "proxy_pool": str(health.get("route_proxy_pool") or ""),
                "priority": "",
                "manual_login_required": False,
            },
            "health": health,
            "lease_display": str(health.get("leased_proxy_server") or "no active lease"),
            "cooldown_display": (
                f"route cooldown ({self._format_elapsed(self._safe_int(health.get('cooldown_remaining_seconds'), 0))})"
                if self._safe_int(health.get("cooldown_remaining_seconds"), 0) > 0
                else "no cooldown"
            ),
            "manual_login_required": False,
        }
        if selected_worker_name == worker_name_clean and (
            not selected_route_name or selected_route_name in {route_name, route_key_name}
        ) and not matched_selected_key:
            return key
        return matched_selected_key

    def _refresh_vps_scraper_tree(self, preserve_selection: bool = True):
        if not self.vps_scraper_tree:
            return

        selected_key = self.selected_vps_scraper_key if preserve_selection else None
        _, selected_worker_name, selected_route_name = self._selected_vps_route_identity() if preserve_selection else (
            None,
            None,
            None,
        )
        matched_selected_key = None
        for item in self.vps_scraper_tree.get_children():
            self.vps_scraper_tree.delete(item)
        self.vps_scraper_records_by_key = {}

        routes, health_by_worker, error = self._fetch_vps_scraper_payloads()
        if error:
            self.status_bar.config(text=error)
            if self.settings_window and self.settings_window.winfo_exists():
                messagebox.showwarning("VPS Scrapers", error)
            return
        self._refresh_vps_worker_summary_tree(routes=routes, health_by_worker=health_by_worker)
        proxy_options = self._build_vps_proxy_profile_options(routes)
        proxy_widget = self.vps_scraper_form_vars.get("proxy_profile_widget")
        if proxy_widget is not None:
            proxy_widget["values"] = proxy_options
            current = self.vps_scraper_form_vars["proxy_profile"].get()
            if current not in proxy_options:
                self.vps_scraper_form_vars["proxy_profile"].set(VPS_PROXY_PROFILE_DIRECT)

        skipped_routes = 0
        rendered_workers: set[str] = set()
        health_by_route_name: dict[str, dict] = {}
        for worker_name, health in (health_by_worker or {}).items():
            route_name = str((health or {}).get("route_name") or "").strip()
            if route_name and route_name not in health_by_route_name:
                health_by_route_name[route_name] = dict(health)
                health_by_route_name[route_name]["_active_worker_name"] = worker_name
        for route in routes:
            try:
                worker_name = str(route.get("legacy_worker_name") or route.get("worker_name") or "").strip() or "central"
                route_name = str(route.get("route_name") or "").strip()
                if not route_name:
                    continue
                base_key = f"route::{route_name}"
                key = base_key
                duplicate_suffix = 2
                while key in self.vps_scraper_records_by_key:
                    key = f"{base_key}::{duplicate_suffix}"
                    duplicate_suffix += 1
                health = health_by_route_name.get(route_name, {})
                active_worker_name = str(health.get("_active_worker_name") or "").strip()
                route_status = str(route.get("status") or "ENABLED").strip().upper() or "ENABLED"
                if health:
                    status = str(health.get("status") or route_status.lower()).strip() or route_status.lower()
                elif route_status == "NEEDS_LOGIN":
                    status = "manual_login_required"
                else:
                    status = route_status.lower()

                lease_text, cooldown_text, _ = self._worker_runtime_summary(health if health else {})
                lease_display = lease_text if health else "-"
                route_cooldown_seconds = self._seconds_until_iso(route.get("cooldown_until"))
                if route_status == "NEEDS_LOGIN":
                    cooldown_display = "manual login required"
                elif route_cooldown_seconds > 0:
                    cooldown_display = f"waiting cooldown ({self._format_elapsed(route_cooldown_seconds)})"
                elif health:
                    cooldown_display = cooldown_text
                else:
                    cooldown_display = "-"
                last_run = str(
                    health.get("last_run_finished_at")
                    or health.get("last_run_started_at")
                    or route.get("last_selected_at")
                    or route.get("updated_at")
                    or ""
                )
                last_run_display = last_run.replace("T", " ")[:19] if last_run else ""
                lane_display = str(route.get("effective_lane") or route.get("computed_lane") or "warm").strip() or "warm"
                if str(route.get("lane_override") or "").strip():
                    lane_display = f"{lane_display}*"
                priority_score = route.get("priority_score")
                score_display = ""
                if priority_score not in (None, ""):
                    try:
                        score_display = f"{float(priority_score):.2f}"
                    except (TypeError, ValueError):
                        score_display = str(priority_score)
                query_count = self._safe_int(route.get("query_count"), -1)
                if query_count < 0:
                    query_count = len(route.get("queries") or [])
                if query_count <= 0:
                    query_csv = str(route.get("search_queries") or "").strip()
                    if query_csv and query_csv.upper() != "BUCKETS":
                        query_count = len([token for token in query_csv.split(",") if token.strip()])

                values = (
                    worker_name,
                    route_name,
                    f"{max(0, query_count)} query(s)",
                    lane_display,
                    score_display,
                    status,
                    str(max(0, self._safe_int(health.get("listings_scraped_last_minute"), 0))) if health else "-",
                    last_run_display,
                )
                try:
                    self.vps_scraper_tree.insert("", tk.END, iid=key, values=values)
                except tk.TclError:
                    # Keep route list rendering resilient if an unexpected duplicate iid slips through.
                    fallback_key = f"{base_key}::{datetime.now().timestamp()}"
                    self.vps_scraper_tree.insert("", tk.END, iid=fallback_key, values=values)
                    key = fallback_key
                self.vps_scraper_records_by_key[key] = {
                    "route": route,
                    "health": health,
                    "lease_display": lease_display,
                    "cooldown_display": cooldown_display,
                    "manual_login_required": route_status == "NEEDS_LOGIN",
                    "active_worker_name": active_worker_name or None,
                }
                rendered_workers.add(worker_name)
                if selected_worker_name == worker_name and selected_route_name == route_name and not matched_selected_key:
                    matched_selected_key = key
            except Exception:
                skipped_routes += 1
                continue

        for worker_name in sorted(health_by_worker):
            if worker_name in rendered_workers:
                continue
            matched_selected_key = self._insert_vps_health_only_row(
                worker_name=worker_name,
                health=health_by_worker.get(worker_name) or {},
                selected_worker_name=selected_worker_name,
                selected_route_name=selected_route_name,
                matched_selected_key=matched_selected_key,
            )

        if matched_selected_key and matched_selected_key in self.vps_scraper_tree.get_children():
            self.vps_scraper_tree.selection_set(matched_selected_key)
            self.vps_scraper_tree.focus(matched_selected_key)
            self.vps_scraper_tree.see(matched_selected_key)
            self.selected_vps_scraper_key = matched_selected_key
        elif selected_key and selected_key in self.vps_scraper_tree.get_children():
            self.vps_scraper_tree.selection_set(selected_key)
            self.vps_scraper_tree.focus(selected_key)
            self.vps_scraper_tree.see(selected_key)
            self.selected_vps_scraper_key = selected_key

        loaded_count = len(self.vps_scraper_records_by_key)
        if skipped_routes > 0:
            self.status_bar.config(text=f"Loaded {loaded_count} VPS scraper route(s), skipped {skipped_routes} malformed row(s)")
        else:
            if not routes and health_by_worker:
                self.status_bar.config(
                    text=(
                        "No saved central routes found. Showing live worker health rows only. "
                        "Create a central route to enable shared scheduling."
                    )
                )
            else:
                self.status_bar.config(text=f"Loaded {loaded_count} central route(s)")

    def _clear_vps_scraper_form(self):
        self.selected_vps_scraper_key = None
        defaults = {
            "worker_name": "",
            "route_name": "",
            "is_enabled": "1",
            "priority": "100",
            "lane_override": "auto",
            "route_interval_seconds": "",
            "user_data_dir": "",
            "proxy_server": "",
            "proxy_username": "",
            "proxy_password": "",
            "proxy_profile": VPS_PROXY_PROFILE_DIRECT,
            "proxy_mode": "fixed",
            "proxy_pool": "",
            "search_queries": "",
            "manual_login_required": "0",
            "manual_login_reason": "",
            "quarantined_at": "",
            "quarantine_reason": "",
            "quarantine_evidence": "",
            "worker_status": "",
            "worker_scraped_last_minute": "",
            "worker_lease": "",
            "worker_cooldown": "",
            "worker_last_run": "",
            "computed_lane": "",
            "effective_lane": "",
            "priority_score": "",
            "priority_score_updated_at": "",
            "profitable_hit_rate": "",
            "recent_duplicate_ratio": "",
        }
        for key, value in defaults.items():
            var = self.vps_scraper_form_vars.get(key)
            if hasattr(var, "set"):
                var.set(value)
        if self.vps_scraper_tree:
            self.vps_scraper_tree.selection_remove(self.vps_scraper_tree.selection())

    def _on_vps_scraper_selected(self, event=None):
        if not self.vps_scraper_tree:
            return
        selection = self.vps_scraper_tree.selection()
        if not selection:
            return

        key = selection[0]
        data = self.vps_scraper_records_by_key.get(key) or {}
        route = data.get("route") or {}
        health = data.get("health") or {}
        synthetic_health_row = bool(route.get("is_synthetic_health_row"))

        self.selected_vps_scraper_key = key
        self.vps_scraper_form_vars["worker_name"].set(
            str(route.get("legacy_worker_name") or route.get("worker_name") or "")
        )
        self.vps_scraper_form_vars["route_name"].set(str(route.get("route_name") or ""))
        self.vps_scraper_form_vars["is_enabled"].set("1" if route.get("is_enabled", True) else "0")
        self.vps_scraper_form_vars["priority"].set("" if synthetic_health_row else str(route.get("priority") or 100))
        lane_override = str(route.get("lane_override") or "").strip().lower() or "auto"
        self.vps_scraper_form_vars["lane_override"].set(lane_override)
        route_interval_value = route.get("route_interval_seconds")
        self.vps_scraper_form_vars["route_interval_seconds"].set(
            str(route_interval_value) if route_interval_value not in (None, "") else ""
        )
        self.vps_scraper_form_vars["user_data_dir"].set(
            str(route.get("user_data_dir") or "") if synthetic_health_row else ""
        )
        self.vps_scraper_form_vars["proxy_mode"].set(str(route.get("proxy_mode") or "fixed"))
        self.vps_scraper_form_vars["proxy_server"].set(str(route.get("proxy_server") or ""))
        self.vps_scraper_form_vars["proxy_username"].set(str(route.get("proxy_username") or ""))
        self.vps_scraper_form_vars["proxy_password"].set(str(route.get("proxy_password") or ""))
        self.vps_scraper_form_vars["proxy_pool"].set(str(route.get("proxy_pool") or ""))
        profile_label = VPS_PROXY_PROFILE_DIRECT
        for label, profile_route in self.vps_proxy_profile_options.items():
            if not profile_route:
                continue
            if (
                str(profile_route.get("proxy_server") or "") == str(route.get("proxy_server") or "")
                and str(profile_route.get("proxy_username") or "") == str(route.get("proxy_username") or "")
                and str(profile_route.get("proxy_password") or "") == str(route.get("proxy_password") or "")
            ):
                profile_label = label
                break
        if str(route.get("proxy_mode") or "fixed").strip().lower() == "auto_rotation":
            profile_label = VPS_PROXY_PROFILE_CUSTOM
        self.vps_scraper_form_vars["proxy_profile"].set(profile_label)
        self.vps_scraper_form_vars["search_queries"].set(str(route.get("search_queries") or ""))
        self.vps_scraper_form_vars["manual_login_required"].set("0")
        self.vps_scraper_form_vars["manual_login_reason"].set("")
        self.vps_scraper_form_vars["quarantined_at"].set("")
        self.vps_scraper_form_vars["quarantine_reason"].set("")
        self.vps_scraper_form_vars["quarantine_evidence"].set("")
        self.vps_scraper_form_vars["worker_status"].set(
            str(health.get("status") or route.get("status") or "unknown")
        )
        self.vps_scraper_form_vars["worker_scraped_last_minute"].set(
            str(max(0, self._safe_int(health.get("listings_scraped_last_minute"), 0)))
        )
        self.vps_scraper_form_vars["worker_lease"].set(str(data.get("lease_display") or "no active lease"))
        self.vps_scraper_form_vars["worker_cooldown"].set(str(data.get("cooldown_display") or "no cooldown"))
        self.vps_scraper_form_vars["computed_lane"].set(str(route.get("computed_lane") or ""))
        self.vps_scraper_form_vars["effective_lane"].set(str(route.get("effective_lane") or ""))
        priority_score = route.get("priority_score")
        self.vps_scraper_form_vars["priority_score"].set(
            f"{float(priority_score):.2f}" if priority_score not in (None, "") else ""
        )
        self.vps_scraper_form_vars["priority_score_updated_at"].set(
            str(route.get("priority_score_updated_at") or "")
        )
        profitable_hit_rate = route.get("profitable_hit_rate")
        self.vps_scraper_form_vars["profitable_hit_rate"].set(
            f"{float(profitable_hit_rate):.2f}" if profitable_hit_rate not in (None, "") else ""
        )
        recent_duplicate_ratio = route.get("recent_duplicate_ratio")
        self.vps_scraper_form_vars["recent_duplicate_ratio"].set(
            f"{float(recent_duplicate_ratio):.2f}" if recent_duplicate_ratio not in (None, "") else ""
        )

        last_run = str(
            health.get("last_run_finished_at")
            or health.get("last_run_started_at")
            or health.get("updated_at")
            or ""
        )
        self.vps_scraper_form_vars["worker_last_run"].set(last_run.replace("T", " ")[:19] if last_run else "")
        if synthetic_health_row:
            route_hint = str(route.get("route_name") or "").strip()
            self.status_bar.config(
                text=(
                    f"{route.get('worker_name')}: showing live worker health only"
                    + (f" for route '{route_hint}'" if route_hint else "")
                    + ". "
                    "Create or save a DB-backed route to populate lane and score fields."
                )
            )
        else:
            active_worker_name = str(data.get("active_worker_name") or "").strip()
            self.status_bar.config(
                text=(
                    f"Central route '{route.get('route_name')}' loaded"
                    + (f" (legacy origin: {route.get('legacy_worker_name')})" if route.get("legacy_worker_name") else "")
                    + (f"; active on {active_worker_name}" if active_worker_name else "")
                )
            )

    def _save_vps_scraper_route(self, allow_remap: bool = True):
        route_name = self.vps_scraper_form_vars["route_name"].get().strip()
        proxy_mode = str(self.vps_scraper_form_vars["proxy_mode"].get() or "").strip().lower()
        if proxy_mode not in {"fixed", "auto_rotation"}:
            proxy_mode = "fixed"
            self.vps_scraper_form_vars["proxy_mode"].set(proxy_mode)
        if proxy_mode == "fixed":
            self._apply_selected_proxy_profile()
        else:
            # In auto rotation mode, route pool selection controls proxy choice.
            self.vps_scraper_form_vars["proxy_profile"].set(VPS_PROXY_PROFILE_CUSTOM)
        proxy_server = self.vps_scraper_form_vars["proxy_server"].get().strip()
        proxy_pool_raw = self.vps_scraper_form_vars["proxy_pool"].get().strip()
        pool_entries = []

        if proxy_mode == "auto_rotation":
            pool_entries = self._parse_socks5_proxy_pool_entries(proxy_pool_raw)
            if not pool_entries:
                pool_entries = self._collect_auto_rotation_proxy_pool()
                if not pool_entries:
                    messagebox.showwarning(
                        "Validation Error",
                        "Auto rotation requires SOCKS5 proxies. Import TXT/CSV or add active SOCKS5 proxies first.",
                    )
                    return False
                random.shuffle(pool_entries)
                proxy_pool_raw = ", ".join(entry["proxy_server"] for entry in pool_entries)
                self.vps_scraper_form_vars["proxy_pool"].set(proxy_pool_raw)

            if not proxy_server and pool_entries:
                seed_entry = random.choice(pool_entries)
                proxy_server = str(seed_entry.get("proxy_server") or "").strip()
                self.vps_scraper_form_vars["proxy_server"].set(proxy_server)
                if not self.vps_scraper_form_vars["proxy_username"].get().strip():
                    self.vps_scraper_form_vars["proxy_username"].set(str(seed_entry.get("proxy_username") or ""))
                if not self.vps_scraper_form_vars["proxy_password"].get().strip():
                    self.vps_scraper_form_vars["proxy_password"].set(str(seed_entry.get("proxy_password") or ""))

        if not route_name:
            messagebox.showwarning("Validation Error", "Route Name is required.")
            return False
        if not re.fullmatch(r"[a-zA-Z0-9_-]+", route_name):
            messagebox.showwarning("Validation Error", "Route Name may only include letters, numbers, '_' and '-'.")
            return False
        priority_raw = self.vps_scraper_form_vars["priority"].get().strip() or "100"
        try:
            priority = int(priority_raw)
        except ValueError:
            messagebox.showwarning("Validation Error", "Priority must be an integer.")
            return False
        if priority < 0:
            messagebox.showwarning("Validation Error", "Priority must be >= 0.")
            return False

        route_interval_raw = self.vps_scraper_form_vars["route_interval_seconds"].get().strip()
        route_interval_seconds = None
        if route_interval_raw:
            try:
                route_interval_seconds = int(route_interval_raw)
            except ValueError:
                messagebox.showwarning("Validation Error", "Route Interval must be an integer (seconds).")
                return False
            if route_interval_seconds < 1:
                messagebox.showwarning("Validation Error", "Route Interval must be >= 1 second.")
                return False

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("VPS Scrapers", error)
            return False

        payload = {
            "is_enabled": self._is_truthy(self.vps_scraper_form_vars["is_enabled"].get()),
            "proxy_server": proxy_server or None,
            "proxy_username": self.vps_scraper_form_vars["proxy_username"].get().strip() or None,
            "proxy_password": self.vps_scraper_form_vars["proxy_password"].get().strip() or None,
            "proxy_mode": proxy_mode,
            "proxy_pool": (proxy_pool_raw or None) if proxy_mode == "auto_rotation" else None,
            "search_queries": self.vps_scraper_form_vars["search_queries"].get().strip() or None,
            "priority": priority,
            "lane_override": (
                None
                if str(self.vps_scraper_form_vars["lane_override"].get() or "").strip().lower() in {"", "auto"}
                else str(self.vps_scraper_form_vars["lane_override"].get() or "").strip().lower()
            ),
            "route_interval_seconds": route_interval_seconds,
        }

        response_payload = {}
        try:
            response = requests.put(
                f"{ctx['base_url']}/routes/{route_name}",
                headers=ctx["headers"],
                json=payload,
                timeout=(8, 30),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
            response_payload = response.json() if response.content else {}
        except requests.HTTPError as exc:
            detail_text = ""
            response = exc.response
            if response is not None:
                status_code = response.status_code
                raw_body = (response.text or "").strip()
                api_detail = ""
                if raw_body:
                    try:
                        parsed = response.json()
                    except Exception:
                        parsed = None
                    if isinstance(parsed, dict):
                        api_detail = str(parsed.get("detail") or "").strip()
                    if not api_detail:
                        api_detail = raw_body[:500]
                detail_text = f"\nHTTP {status_code}: {api_detail or 'No response body'}"
            messagebox.showerror("Save Failed", f"Failed to save VPS scraper route:\n{exc}{detail_text}")
            self.status_bar.config(text="Failed to save VPS scraper route")
            return False
        except Exception as exc:
            messagebox.showerror("Save Failed", f"Failed to save VPS scraper route:\n{exc}")
            self.status_bar.config(text="Failed to save VPS scraper route")
            return False

        warnings = response_payload.get("warnings") if isinstance(response_payload, dict) else None
        if warnings:
            self._show_vps_route_warnings(warnings)
        self.selected_vps_scraper_key = f"route::{route_name}"
        self._refresh_vps_scraper_tree(preserve_selection=True)
        self.status_bar.config(text=f"Saved central route {route_name}")
        return True

    def _delete_vps_scraper_route(self):
        route_name = self.vps_scraper_form_vars["route_name"].get().strip()
        
        if not route_name:
            messagebox.showwarning("Delete Failed", "Cannot delete. Route Name must be specified.")
            return False

        if not messagebox.askyesno("Confirm Delete", f"Are you sure you want to delete central route '{route_name}'?"):
            return False

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("VPS Scrapers", error)
            return False

        try:
            response = requests.delete(
                f"{ctx['base_url']}/routes/{route_name}",
                headers=ctx["headers"],
                timeout=(8, 30),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
        except requests.HTTPError as exc:
            messagebox.showerror("Delete Failed", f"Failed to delete VPS scraper route:\n{exc}")
            self.status_bar.config(text=f"Failed to delete central route {route_name}")
            return False
        except Exception as exc:
            messagebox.showerror("Delete Failed", f"Failed to delete VPS scraper route:\n{exc}")
            self.status_bar.config(text=f"Failed to delete central route {route_name}")
            return False

        self.selected_vps_scraper_key = None
        self._clear_vps_scraper_form()
        self._refresh_vps_scraper_tree(preserve_selection=False)
        self.status_bar.config(text=f"Deleted central route {route_name}")
        return True

    def _delete_selected_vps_scraper_route(self):
        key, worker_name, route_name = self._selected_vps_route_identity()
        if not key or not route_name:
            messagebox.showwarning("No Selection", "Select a VPS scraper route first.")
            return

        if not messagebox.askyesno(
            "Delete Central Route",
            f"Delete central route '{route_name}'?",
        ):
            return

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("VPS Scrapers", error)
            return

        response_payload = {}
        try:
            response = requests.delete(
                f"{ctx['base_url']}/routes/{route_name}",
                headers=ctx["headers"],
                timeout=(8, 25),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
            response_payload = response.json() if response.content else {}
        except Exception as exc:
            messagebox.showerror("Delete Failed", f"Failed to delete VPS scraper route:\n{exc}")
            return

        warnings = response_payload.get("warnings") if isinstance(response_payload, dict) else None
        if warnings:
            self._show_vps_route_warnings(warnings)
        self._clear_vps_scraper_form()
        self._refresh_vps_scraper_tree(preserve_selection=False)
        self.status_bar.config(text=f"Deleted central route {route_name}")

    def _request_vps_route_retest(self):
        key, worker_name, route_name = self._selected_vps_route_identity()
        if not key or not worker_name or not route_name:
            messagebox.showwarning("No Selection", "Select a VPS scraper route first.")
            return
        route_record = self.vps_scraper_records_by_key.get(key) or {}
        route = route_record.get("route") or {}
        if not bool(route.get("is_synthetic_health_row")):
            messagebox.showinfo(
                "Execution Profiles",
                "Central routes are retried automatically by the scheduler. "
                "Manual retest and manual-login recovery are managed on execution profiles, not on central routes.",
            )
            return

        if not messagebox.askyesno(
            "Retest Route",
            (
                f"Request retest for route '{route_name}' on worker '{worker_name}'?\n\n"
                "This clears manual-login lock and schedules the route for immediate retry."
            ),
        ):
            return

        reason = simpledialog.askstring(
            "Retest Reason (Optional)",
            "Reason to record for this retest request:",
            parent=self.settings_window or self.root,
        )

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("VPS Scrapers", error)
            return

        response_payload = {}
        try:
            response = requests.post(
                f"{ctx['base_url']}/worker-routes/{worker_name}/{route_name}/retest",
                headers=ctx["headers"],
                json={"reason": (reason or "").strip() or None},
                timeout=(8, 30),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
            response_payload = response.json() if response.content else {}
        except Exception as exc:
            messagebox.showerror("Retest Failed", f"Failed to request route retest:\n{exc}")
            return

        warnings = response_payload.get("warnings") if isinstance(response_payload, dict) else None
        if warnings:
            self._show_vps_route_warnings(warnings)
        self.selected_vps_scraper_key = key
        self._refresh_vps_scraper_tree(preserve_selection=True)
        self.status_bar.config(text=f"Retest requested for VPS route {worker_name}/{route_name}")

    def _bulk_clear_vps_worker_manual_login(self):
        _, worker_name, _ = self._selected_vps_route_identity()
        selected_key = self.selected_vps_scraper_key
        route_record = self.vps_scraper_records_by_key.get(selected_key or "") or {}
        route = route_record.get("route") or {}
        if selected_key and not bool(route.get("is_synthetic_health_row")):
            messagebox.showinfo(
                "Execution Profiles",
                "Manual-login state is now tracked on execution profiles. "
                "Use the execution-profile health view or API workflow for profile quarantine recovery.",
            )
            return
        worker_name = str(worker_name or "").strip()
        if not worker_name:
            worker_name_var = self.vps_scraper_form_vars.get("worker_name")
            worker_name = worker_name_var.get().strip() if worker_name_var is not None else ""
        if not worker_name:
            messagebox.showwarning("Missing Worker", "Select a route or set Worker Name first.")
            return

        if not messagebox.askyesno(
            "Bulk Clear Manual Login",
            (
                f"Clear manual-login lock for all routes on worker '{worker_name}'?\n\n"
                "Use this when a shared dependency/login event quarantined multiple routes."
            ),
        ):
            return

        reason = simpledialog.askstring(
            "Bulk Clear Reason (Optional)",
            "Reason to record for this bulk clear action:",
            parent=self.settings_window or self.root,
        )

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("VPS Scrapers", error)
            return

        response_payload = {}
        try:
            response = requests.post(
                f"{ctx['base_url']}/worker-routes/{worker_name}/bulk-clear-manual-login",
                headers=ctx["headers"],
                json={"reason": (reason or "").strip() or None},
                timeout=(8, 30),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            response.raise_for_status()
            response_payload = response.json() if response.content else {}
        except Exception as exc:
            messagebox.showerror("Bulk Clear Failed", f"Failed to clear manual-login routes:\n{exc}")
            return

        cleared_count = self._safe_int(
            (response_payload or {}).get("cleared_count", 0),
            0,
        )
        warnings = response_payload.get("warnings") if isinstance(response_payload, dict) else None
        if warnings:
            self._show_vps_route_warnings(warnings)
        self._refresh_vps_scraper_tree(preserve_selection=True)
        self.status_bar.config(
            text=f"Cleared manual-login locks for {cleared_count} route(s) on {worker_name}"
        )

    def _save_vps_connection_settings(self):
        accessory_keywords_raw = self.vps_scraper_form_vars["accessory_filter_keywords"].get().strip()
        accessory_keywords = self._parse_accessory_keyword_csv(accessory_keywords_raw)
        if not accessory_keywords:
            accessory_keywords = self._parse_accessory_keyword_csv(self._default_accessory_keyword_csv())
        accessory_keywords_csv = ", ".join(accessory_keywords)

        accessory_max_price_raw = self.vps_scraper_form_vars["accessory_filter_max_price"].get().strip() or "120"
        accessory_max_price = self._safe_float(accessory_max_price_raw)
        if accessory_max_price is None or accessory_max_price <= 0:
            messagebox.showwarning(
                "Validation Error",
                "Accessory max price must be a number greater than 0.",
            )
            return
        accessory_max_price_text = f"{float(accessory_max_price):.2f}".rstrip("0").rstrip(".")
        normalized_monitor_services = self._normalize_server_monitor_services(
            self.vps_scraper_form_vars["server_monitor_worker_services"].get()
        )
        self.vps_scraper_form_vars["server_monitor_worker_services"].set(normalized_monitor_services)
        proxy_rotation_period_seconds = max(
            0,
            self._safe_int(self.vps_scraper_form_vars["server_proxy_rotation_period_seconds"].get(), 120),
        )
        proxy_lease_seconds = max(
            30,
            self._safe_int(self.vps_scraper_form_vars["server_proxy_lease_seconds"].get(), 600),
        )
        worker_1_scrape_frequency_seconds = max(
            1,
            self._safe_int(self.vps_scraper_form_vars["server_worker_1_scrape_frequency_seconds"].get(), 8),
        )
        worker_2_scrape_frequency_seconds = max(
            1,
            self._safe_int(self.vps_scraper_form_vars["server_worker_2_scrape_frequency_seconds"].get(), 8),
        )
        worker_3_scrape_frequency_seconds = max(
            1,
            self._safe_int(self.vps_scraper_form_vars["server_worker_3_scrape_frequency_seconds"].get(), 5),
        )
        self.vps_scraper_form_vars["server_proxy_rotation_period_seconds"].set(str(proxy_rotation_period_seconds))
        self.vps_scraper_form_vars["server_proxy_lease_seconds"].set(str(proxy_lease_seconds))
        self.vps_scraper_form_vars["server_worker_1_scrape_frequency_seconds"].set(
            str(worker_1_scrape_frequency_seconds)
        )
        self.vps_scraper_form_vars["server_worker_2_scrape_frequency_seconds"].set(
            str(worker_2_scrape_frequency_seconds)
        )
        self.vps_scraper_form_vars["server_worker_3_scrape_frequency_seconds"].set(
            str(worker_3_scrape_frequency_seconds)
        )

        updates = {
            "server_sync_enabled": "1" if self._is_truthy(self.vps_scraper_form_vars["server_sync_enabled"].get()) else "0",
            "server_api_base_url": self._normalize_server_base_url(self.vps_scraper_form_vars["server_api_base_url"].get()),
            "server_api_token": self.vps_scraper_form_vars["server_api_token"].get().strip(),
            "server_sync_poll_seconds": str(max(1, self._safe_int(self.vps_scraper_form_vars["server_sync_poll_seconds"].get(), 2))),
            "server_proxy_rotation_period_seconds": str(proxy_rotation_period_seconds),
            "server_proxy_lease_seconds": str(proxy_lease_seconds),
            "server_worker_1_scrape_frequency_seconds": str(worker_1_scrape_frequency_seconds),
            "server_worker_2_scrape_frequency_seconds": str(worker_2_scrape_frequency_seconds),
            "server_worker_3_scrape_frequency_seconds": str(worker_3_scrape_frequency_seconds),
            # Keep legacy key populated for backward compatibility with older builds.
            "server_profile_rotation_period_seconds": str(worker_1_scrape_frequency_seconds),
            "server_ssh_enabled": "1" if self._is_truthy(self.vps_scraper_form_vars["server_ssh_enabled"].get()) else "0",
            "server_ssh_user": self.vps_scraper_form_vars["server_ssh_user"].get().strip() or "ubuntu",
            "server_ssh_host": self.vps_scraper_form_vars["server_ssh_host"].get().strip(),
            "server_ssh_project_dir": self.vps_scraper_form_vars["server_ssh_project_dir"].get().strip() or "/home/ubuntu/iphone-flipper-server/server",
            "server_monitor_worker_services": normalized_monitor_services,
            "accessory_filter_keywords": accessory_keywords_csv,
            "accessory_filter_max_price": accessory_max_price_text,
        }

        if not updates["server_api_base_url"]:
            messagebox.showwarning("Validation Error", "Server API Base URL is required.")
            return

        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for key, value in updates.items():
            cursor.execute(
                """
                INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """,
                (key, value, now_iso),
            )
        conn.commit()
        conn.close()

        self.vps_scraper_form_vars["accessory_filter_keywords"].set(accessory_keywords_csv)
        self.vps_scraper_form_vars["accessory_filter_max_price"].set(accessory_max_price_text)
        self._start_or_restart_server_sync_from_settings()
        self._refresh_vps_scraper_tree(preserve_selection=True)
        self._sync_accessory_filter_settings_to_vps(accessory_keywords_csv, accessory_max_price_text)
        self._apply_vps_rotation_defaults_to_vps(
            proxy_rotation_period_seconds=proxy_rotation_period_seconds,
            proxy_lease_seconds=proxy_lease_seconds,
            worker_1_scrape_frequency_seconds=worker_1_scrape_frequency_seconds,
            worker_2_scrape_frequency_seconds=worker_2_scrape_frequency_seconds,
            worker_3_scrape_frequency_seconds=worker_3_scrape_frequency_seconds,
        )
        self.status_bar.config(text="VPS connection settings saved")

    def _apply_vps_rotation_defaults_to_vps(
        self,
        proxy_rotation_period_seconds: int,
        proxy_lease_seconds: int,
        worker_1_scrape_frequency_seconds: int,
        worker_2_scrape_frequency_seconds: int,
        worker_3_scrape_frequency_seconds: int,
    ):
        ctx, error = self._get_server_ssh_context()
        if error:
            self.status_bar.config(text=f"Saved locally; VPS rotation defaults not applied ({error})")
            return

        project_q = shlex.quote(ctx["project_dir"])
        updates_q = shlex.quote(
            json.dumps(
                {
                    "WORKER_PROXY_REUSE_COOLDOWN_SECONDS": str(max(0, int(proxy_rotation_period_seconds))),
                    "WORKER_PROXY_LEASE_SECONDS": str(max(30, int(proxy_lease_seconds))),
                    "SCRAPE_INTERVAL_SECONDS": str(max(1, int(worker_1_scrape_frequency_seconds))),
                    "SCRAPE_INTERVAL_SECONDS_WORKER_2": str(max(1, int(worker_2_scrape_frequency_seconds))),
                    "SCRAPE_INTERVAL_SECONDS_WORKER_3": str(max(1, int(worker_3_scrape_frequency_seconds))),
                }
            )
        )
        remote_cmd = (
            "bash -lc "
            + shlex.quote(
                f"""
                set -e
                export PROJECT_DIR={project_q}
                export UPDATES_JSON={updates_q}
                python3 - <<'PY'
import json
import os
from pathlib import Path

project_dir = Path(os.environ["PROJECT_DIR"])
env_path = project_dir / ".env"
updates = json.loads(os.environ["UPDATES_JSON"])
if not env_path.exists():
    raise SystemExit(f"Missing env file: {{env_path}}")

lines = env_path.read_text(encoding="utf-8").splitlines()
index_by_key = {{}}
for idx, line in enumerate(lines):
    raw = line.strip()
    if not raw or raw.startswith("#") or "=" not in line:
        continue
    key = line.split("=", 1)[0].strip()
    if key:
        index_by_key[key] = idx

for key, value in updates.items():
    entry = f"{{key}}={{value}}"
    if key in index_by_key:
        lines[index_by_key[key]] = entry
    else:
        lines.append(entry)

env_path.write_text("\\n".join(lines).rstrip() + "\\n", encoding="utf-8")
print("Updated worker rotation defaults in .env:", ", ".join(f"{{k}}={{v}}" for k, v in updates.items()))
PY
                cd "$PROJECT_DIR/infra"
                docker compose --env-file ../.env up -d worker worker_2 worker_3
                docker compose --env-file ../.env ps worker worker_2 worker_3
                """
            )
        )
        if not (self.server_monitor_window and self.server_monitor_window.winfo_exists()):
            self.show_server_activity_monitor()
        self._run_server_ssh_command("Apply Rotation Defaults", remote_cmd, timeout_seconds=120)

    @staticmethod
    def _parse_vps_proxy_server(proxy_server: str, username: str, password: str) -> dict:
        base = (proxy_server or "").strip()
        parsed = urlparse(base)
        if not parsed.scheme or not parsed.hostname or not parsed.port:
            raise ValueError("Proxy Server must be in format: scheme://host:port")

        effective_user = (username or "").strip() or (parsed.username or "")
        effective_password = (password or "").strip() or (parsed.password or "")
        return {
            "scheme": str(parsed.scheme).lower(),
            "host": str(parsed.hostname),
            "port": int(parsed.port),
            "username": effective_user,
            "password": effective_password,
        }

    @staticmethod
    def _container_to_host_profile_path(profile_dir: str, project_dir: str) -> str:
        raw_profile = (profile_dir or "").strip()
        if not raw_profile:
            return ""
        if raw_profile.startswith("/app/runtime/"):
            suffix = raw_profile[len("/app/runtime/"):]
            return str(Path(project_dir) / "runtime" / suffix)
        return raw_profile

    def _start_vps_manual_login(self):
        proxy_mode_var = self.vps_scraper_form_vars.get("proxy_mode")
        proxy_mode = str(proxy_mode_var.get() if proxy_mode_var is not None else "fixed").strip().lower()
        if proxy_mode == "fixed":
            self._apply_selected_proxy_profile()
        worker_name = self.vps_scraper_form_vars["worker_name"].get().strip()
        if not worker_name:
            worker_name = self._suggest_next_worker_name()
            self.vps_scraper_form_vars["worker_name"].set(worker_name)
        profile_dir = self.vps_scraper_form_vars["user_data_dir"].get().strip()
        proxy_server = self.vps_scraper_form_vars["proxy_server"].get().strip()
        proxy_username = self.vps_scraper_form_vars["proxy_username"].get().strip()
        proxy_password = self.vps_scraper_form_vars["proxy_password"].get().strip()
        if proxy_mode == "auto_rotation" and not proxy_server:
            pool_entries = self._parse_socks5_proxy_pool_entries(self.vps_scraper_form_vars["proxy_pool"].get().strip())
            if not pool_entries:
                pool_entries = self._collect_auto_rotation_proxy_pool()
            if pool_entries:
                seed_entry = random.choice(pool_entries)
                proxy_server = str(seed_entry.get("proxy_server") or "").strip()
                proxy_username = str(seed_entry.get("proxy_username") or "").strip()
                proxy_password = str(seed_entry.get("proxy_password") or "").strip()
                self.vps_scraper_form_vars["proxy_server"].set(proxy_server)
                self.vps_scraper_form_vars["proxy_username"].set(proxy_username)
                self.vps_scraper_form_vars["proxy_password"].set(proxy_password)
        if not profile_dir:
            profile_dir = self._suggest_profile_dir_for_worker(worker_name)
            self.vps_scraper_form_vars["user_data_dir"].set(profile_dir)
        normalized_worker, remapped = self._normalize_worker_name_for_rotation(worker_name)
        if remapped and normalized_worker:
            worker_name = normalized_worker
            self.vps_scraper_form_vars["worker_name"].set(worker_name)
        route_name = self.vps_scraper_form_vars["route_name"].get().strip()
        if not route_name:
            route_name = self._suggest_route_name_for_worker(worker_name, profile_dir)
            self.vps_scraper_form_vars["route_name"].set(route_name)
        # Ensure this manual-login target is persisted in route registry immediately.
        if not self._save_vps_scraper_route(allow_remap=False):
            return

        ctx, error = self._get_server_ssh_context()
        if error:
            messagebox.showwarning("VPS Login", error)
            return

        host_profile = self._container_to_host_profile_path(profile_dir, ctx["project_dir"])
        if proxy_server:
            try:
                proxy_parts = self._parse_vps_proxy_server(proxy_server, proxy_username, proxy_password)
            except ValueError as exc:
                messagebox.showwarning("Validation Error", str(exc))
                return

            direct_proxy = f"{proxy_parts['scheme']}://{proxy_parts['host']}:{proxy_parts['port']}"
            bridge_required = bool(proxy_parts["username"] or proxy_parts["password"])
            if bridge_required:
                auth_user = quote(str(proxy_parts["username"] or ""), safe="")
                auth_password = quote(str(proxy_parts["password"] or ""), safe="")
                auth_fragment = f"#{auth_user}:{auth_password}" if auth_user or auth_password else ""
                upstream_proxy = f"{direct_proxy}{auth_fragment}"
            else:
                upstream_proxy = direct_proxy
        else:
            direct_proxy = ""
            upstream_proxy = ""
            bridge_required = False

        profile_q = shlex.quote(host_profile)
        direct_proxy_q = shlex.quote(direct_proxy)
        upstream_proxy_q = shlex.quote(upstream_proxy)
        bridge_required_q = shlex.quote("1" if bridge_required else "0")
        login_url_q = shlex.quote("https://www.facebook.com/login")

        remote_cmd = (
            "bash -lc "
            + shlex.quote(
                f"""
                set -e
                PROFILE={profile_q}
                DIRECT_PROXY={direct_proxy_q}
                UPSTREAM_PROXY={upstream_proxy_q}
                BRIDGE_REQUIRED={bridge_required_q}
                LOGIN_URL={login_url_q}
                PROXY="$DIRECT_PROXY"
                mkdir -p "$PROFILE"
                if [ ! -w "$PROFILE" ]; then
                  if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
                    sudo chown -R "$(id -un):$(id -gn)" "$PROFILE" >/dev/null 2>&1 || true
                  fi
                fi
                chmod -R u+rwX "$PROFILE" >/dev/null 2>&1 || true
                BROWSER=""
                for c in google-chrome google-chrome-stable chromium chromium-browser; do
                  if command -v "$c" >/dev/null 2>&1; then
                    BROWSER="$c"
                    break
                  fi
                done
                if [ -z "$BROWSER" ]; then
                  echo "No Chromium browser found on VPS host."
                  echo "Install chromium or google-chrome, then retry."
                  exit 1
                fi

                if [ "$BRIDGE_REQUIRED" = "1" ]; then
                  if ! command -v python3 >/dev/null 2>&1; then
                    echo "python3 is required for proxy auth bridge setup."
                    exit 1
                  fi

                  INSTALL_LOG=/tmp/iphone_flipper_vps_bridge_install.log
                  PPROXY_VENV="$HOME/.local/share/iphone_flipper/pproxy-venv"
                  if [ ! -x "$PPROXY_VENV/bin/python3" ]; then
                    mkdir -p "$(dirname "$PPROXY_VENV")"
                    if ! python3 -m venv "$PPROXY_VENV" >"$INSTALL_LOG" 2>&1; then
                      if command -v sudo >/dev/null 2>&1 && sudo -n true >/dev/null 2>&1; then
                        echo "Installing python3-venv for proxy auth bridge..."
                        if ! sudo apt-get update >>"$INSTALL_LOG" 2>&1; then
                          echo "Failed to run apt-get update for python3-venv install."
                          echo "Install log: $INSTALL_LOG"
                          exit 1
                        fi
                        if ! sudo apt-get install -y python3-venv python3-pip >>"$INSTALL_LOG" 2>&1; then
                          echo "Failed to install python3-venv/python3-pip."
                          echo "Install log: $INSTALL_LOG"
                          exit 1
                        fi
                        if ! python3 -m venv "$PPROXY_VENV" >>"$INSTALL_LOG" 2>&1; then
                          echo "Failed to create proxy bridge venv."
                          echo "Install log: $INSTALL_LOG"
                          exit 1
                        fi
                      else
                        echo "python3-venv is required for proxy auth bridge setup."
                        echo "Install it on VPS: sudo apt-get install -y python3-venv python3-pip"
                        exit 1
                      fi
                    fi
                  fi

                  if ! "$PPROXY_VENV/bin/python3" - <<'PY'
import importlib.util
import sys
sys.exit(0 if importlib.util.find_spec("pproxy") else 1)
PY
                  then
                    echo "Installing pproxy for SOCKS5 auth bridge..."
                    if ! "$PPROXY_VENV/bin/python3" -m pip install --quiet pproxy >"$INSTALL_LOG" 2>&1; then
                      echo "Failed to install pproxy."
                      echo "Install log: $INSTALL_LOG"
                      exit 1
                    fi
                  fi

                  BRIDGE_PORT=$(python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
)
                  BRIDGE_PROXY="http://127.0.0.1:$BRIDGE_PORT"
                  BRIDGE_LOG="/tmp/iphone_flipper_vps_proxy_bridge_${{BRIDGE_PORT}}.log"
                  nohup "$PPROXY_VENV/bin/python3" -c 'import asyncio,pproxy.server as s;asyncio.set_event_loop(asyncio.new_event_loop());s.main()' -l "$BRIDGE_PROXY" -r "$UPSTREAM_PROXY" -ul "$BRIDGE_PROXY" -ur "$UPSTREAM_PROXY" >"$BRIDGE_LOG" 2>&1 &
                  BRIDGE_PID=$!
                  sleep 1
                  if ! kill -0 "$BRIDGE_PID" >/dev/null 2>&1; then
                    echo "Failed to start proxy auth bridge."
                    echo "Bridge log: $BRIDGE_LOG"
                    tail -n 40 "$BRIDGE_LOG" || true
                    exit 1
                  fi
                  PROXY="$BRIDGE_PROXY"
                  echo "Proxy auth bridge started: $PROXY (pid=$BRIDGE_PID)"
                  echo "Bridge log: $BRIDGE_LOG"
                fi

                echo "Profile path: $PROFILE"
                if [ -n "$PROXY" ]; then
                  echo "Proxy route: $PROXY"
                  IP_CHECK=$(curl -m 12 -s --proxy "$PROXY" https://ipv4.icanhazip.com || true)
                  if [ -n "$IP_CHECK" ]; then
                    echo "Proxy egress IP: $(echo "$IP_CHECK" | tr -d '\\n')"
                  else
                    echo "Warning: Proxy egress IP check failed (curl via proxy returned no response)."
                  fi
                else
                  echo "Proxy route: Dolphin/profile-bound or direct network (no route override)"
                fi
                COOKIE_STATUS=$(python3 - "$PROFILE" <<'PY'
import sqlite3
import sys
from pathlib import Path

profile = Path(sys.argv[1])
cookie_paths = [
    profile / "Default" / "Cookies",
    profile / "Default" / "Network" / "Cookies",
]
cookie_present = False
permission_issue = False
for db_path in cookie_paths:
    try:
        exists = db_path.exists()
    except PermissionError:
        permission_issue = True
        continue
    except OSError:
        continue
    if not exists:
        continue
    try:
        conn = sqlite3.connect("file:" + str(db_path) + "?mode=ro", uri=True)
        cursor = conn.cursor()
        cursor.execute(
            "SELECT 1 FROM cookies WHERE host_key LIKE '%facebook.com' AND name='c_user' LIMIT 1"
        )
        cookie_present = cursor.fetchone() is not None
        conn.close()
        if cookie_present:
            break
    except Exception:
        continue

if cookie_present:
    print("Facebook c_user cookie present before launch: yes")
elif permission_issue:
    print("Facebook c_user cookie present before launch: unknown (permission denied)")
else:
    print("Facebook c_user cookie present before launch: no")
PY
)
                echo "$COOKIE_STATUS"

                DISPLAY_SOURCE="session-env"
                if [ -z "${{DISPLAY:-}}" ]; then
                  DETECTED_DISPLAY=$(ps -eo args | sed -n 's/.*Xtigervnc \\(:[0-9][0-9]*\\).*/\\1/p' | head -n 1)
                  if [ -z "$DETECTED_DISPLAY" ] && [ -S /tmp/.X11-unix/X1 ]; then
                    DETECTED_DISPLAY=":1"
                  fi
                  if [ -n "$DETECTED_DISPLAY" ]; then
                    export DISPLAY="$DETECTED_DISPLAY"
                    DISPLAY_SOURCE="auto-vnc-detect"
                  fi
                fi

                if [ -z "${{XAUTHORITY:-}}" ] && [ -f "$HOME/.Xauthority" ]; then
                  export XAUTHORITY="$HOME/.Xauthority"
                fi

                if [ -n "$PROXY" ]; then
                  CMD="$BROWSER --user-data-dir=\\"$PROFILE\\" --proxy-server=\\"$PROXY\\" \\"$LOGIN_URL\\""
                else
                  CMD="$BROWSER --user-data-dir=\\"$PROFILE\\" \\"$LOGIN_URL\\""
                fi
                if [ -z "${{DISPLAY:-}}" ]; then
                  echo "DISPLAY is not set and no VNC display was detected."
                  echo "Run this command inside your VPS desktop/VNC session:"
                  echo "DISPLAY=:1 XAUTHORITY=$HOME/.Xauthority $CMD"
                  exit 0
                fi
                echo "Using DISPLAY=$DISPLAY (source: $DISPLAY_SOURCE)"
                if [ -n "${{XAUTHORITY:-}}" ]; then
                  echo "Using XAUTHORITY=$XAUTHORITY"
                fi
                if [ -n "$PROXY" ]; then
                  nohup "$BROWSER" --user-data-dir="$PROFILE" --proxy-server="$PROXY" "$LOGIN_URL" >/tmp/iphone_flipper_vps_manual_login.log 2>&1 &
                else
                  nohup "$BROWSER" --user-data-dir="$PROFILE" "$LOGIN_URL" >/tmp/iphone_flipper_vps_manual_login.log 2>&1 &
                fi
                echo "Manual login browser launched on VPS."
                echo "Launch command: $CMD"
                echo "Log: /tmp/iphone_flipper_vps_manual_login.log"
                echo "After login is complete and browser is closed, click Manual Login again to confirm cookie status turns to 'yes'."
                """
            )
        )
        if not (self.server_monitor_window and self.server_monitor_window.winfo_exists()):
            self.show_server_activity_monitor()
        self._run_server_ssh_command("VPS Manual Login (SOCKS5)", remote_cmd, timeout_seconds=60)
        self.status_bar.config(text="Triggered VPS manual login command")

    def show_server_activity_monitor(self):
        if self.server_monitor_window and self.server_monitor_window.winfo_exists():
            self.server_monitor_window.lift()
            self.server_monitor_window.focus_force()
            return

        window = tk.Toplevel(self.root)
        window.title("VPS Scraper Activity Monitor")
        window.geometry("980x680")
        self.server_monitor_window = window

        def on_close():
            self._stop_server_log_stream(update_status=False)
            self.server_monitor_window = None
            self.server_monitor_text = None
            window.destroy()

        window.protocol("WM_DELETE_WINDOW", on_close)

        controls = ttk.Frame(window)
        controls.pack(fill=tk.X, padx=10, pady=(10, 6))

        ttk.Button(controls, text="API Health", command=self.check_server_api_health).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Worker Status", command=self.check_server_worker_status).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Worker Logs (Live)", command=self.check_server_worker_logs).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Stop Logs", command=self._stop_server_log_stream).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Listing Count", command=self.check_server_listing_count).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Send Telegram Test", command=self.send_server_telegram_test).pack(side=tk.LEFT, padx=4)
        ttk.Button(controls, text="Clear Output", command=self._clear_server_monitor_output).pack(side=tk.RIGHT, padx=4)

        hint = ttk.Label(
            window,
            text=(
                "Uses Scraper settings: Server API Base URL + Token, "
                "Server SSH Host/User/Project Path, and Worker Services."
            ),
        )
        hint.pack(fill=tk.X, padx=10, pady=(0, 6))

        text = scrolledtext.ScrolledText(window, wrap=tk.WORD, font=("Menlo", 10))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        text.config(state=tk.DISABLED)
        self.server_monitor_text = text
         
    # ===== Listings Tab Functions =====

    def _on_listing_left_click(self, event):
        row_id = self.listings_tree.identify_row(event.y)
        self.listing_drag_anchor = row_id or None

    def _on_listing_drag_select(self, event):
        if not self.listing_drag_anchor:
            return
        row_id = self.listings_tree.identify_row(event.y)
        if not row_id:
            return
        children = list(self.listings_tree.get_children())
        if self.listing_drag_anchor not in children or row_id not in children:
            return
        start = children.index(self.listing_drag_anchor)
        end = children.index(row_id)
        if start > end:
            start, end = end, start
        self.listings_tree.selection_set(children[start : end + 1])
        self.listings_tree.focus(row_id)

    def _get_selected_listing_ids(self) -> list[str]:
        """Return selected listing IDs, or focused row as fallback."""
        selection = self.listings_tree.selection()
        if selection:
            selected_ids: list[str] = []
            for item_id in selection:
                values = self.listings_tree.item(item_id).get("values", [])
                if values:
                    selected_ids.append(str(values[0]))
            return selected_ids

        focused = self.listings_tree.focus()
        if not focused:
            return []
        values = self.listings_tree.item(focused).get("values", [])
        if not values:
            return []
        return [str(values[0])]

    def _get_active_listing_id(self):
        """Get the selected/focused listing ID from the listings tree."""
        selected_ids = self._get_selected_listing_ids()
        if not selected_ids:
            return None
        return selected_ids[0]

    def _load_available_listing_models(self) -> list[str]:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT DISTINCT model
            FROM listings
            WHERE model IS NOT NULL
              AND TRIM(model) != ''
              AND LOWER(TRIM(model)) != 'unknown'
            ORDER BY model
            """
        )
        rows = cursor.fetchall()
        conn.close()
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def _update_model_filter_button_text(self):
        selected = sorted(self.model_filter_selected_models)
        if not selected:
            self.model_filter_button_text.set("All Models")
            return
        if len(selected) == 1:
            self.model_filter_button_text.set(selected[0])
            return
        self.model_filter_button_text.set(f"{len(selected)} models")

    def _on_model_filter_changed(self):
        selected = {
            model
            for model, var in self.model_filter_vars.items()
            if bool(var.get())
        }
        self.model_filter_selected_models = selected
        self._update_model_filter_button_text()
        self.refresh_listings()

    def _select_all_model_filters(self):
        for var in self.model_filter_vars.values():
            var.set(True)
        self._on_model_filter_changed()

    def _clear_model_filters(self):
        for var in self.model_filter_vars.values():
            var.set(False)
        self._on_model_filter_changed()

    def _refresh_model_filter_menu(self):
        if not hasattr(self, "model_filter_menu") or self.model_filter_menu is None:
            return

        models = self._load_available_listing_models()
        self.model_filter_selected_models = {
            model for model in self.model_filter_selected_models if model in models
        }
        self.model_filter_vars = {}
        self.model_filter_menu.delete(0, tk.END)
        self.model_filter_menu.add_command(label="Select All", command=self._select_all_model_filters)
        self.model_filter_menu.add_command(label="Clear", command=self._clear_model_filters)
        if models:
            self.model_filter_menu.add_separator()
        for model in models:
            var = tk.BooleanVar(value=model in self.model_filter_selected_models)
            self.model_filter_vars[model] = var
            self.model_filter_menu.add_checkbutton(
                label=model,
                variable=var,
                command=self._on_model_filter_changed,
            )
        self._update_model_filter_button_text()

    def clear_advanced_listing_filters(self):
        self.filter_var.set("all")
        self.min_profit_filter_var.set("")
        self.max_profit_filter_var.set("")
        self.min_price_filter_var.set("")
        self._clear_model_filters()

    def _mark_listing_as_opened(self, listing_id: str):
        if not listing_id:
            return
        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE listings
            SET opened_at = COALESCE(opened_at, ?), updated_at = ?
            WHERE id = ?
            """,
            (now_iso, now_iso, listing_id),
        )
        conn.commit()
        conn.close()
    
    def refresh_listings(self):
        """Refresh the listings table."""
        self.status_bar.config(text="Loading listings...")
        self.root.update()
        self._refresh_model_filter_menu()
        selection_state = desktop_sync.capture_treeview_listing_state(self.listings_tree)
        
        # Clear existing items
        for item in self.listings_tree.get_children():
            self.listings_tree.delete(item)
        
        # Get filter
        filter_type = self.filter_var.get()
        
        # Query database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        query = """
            SELECT
                id,
                title,
                price,
                model,
                condition,
                max_buy_price,
                potential_profit,
                status,
                conversion_score,
                user_flag,
                opened_at
            FROM listings
        """
        where_clauses: list[str] = [
            "model IS NOT NULL",
            "TRIM(model) != ''",
            "LOWER(TRIM(model)) != 'unknown'",
        ]
        params: list = []
        
        if filter_type == "new":
            where_clauses.append("status = 'new'")
        elif filter_type == "high_profit":
            where_clauses.append("potential_profit > 100")
        elif filter_type == "iphone14plus":
            where_clauses.append("(model LIKE '%iPhone 14%' OR model LIKE '%iPhone 15%')")

        selected_models = sorted(self.model_filter_selected_models)
        if selected_models:
            placeholders = ",".join("?" for _ in selected_models)
            where_clauses.append(f"model IN ({placeholders})")
            params.extend(selected_models)

        min_price = self._safe_float(self.min_price_filter_var.get().strip())
        if min_price is not None:
            where_clauses.append("price IS NOT NULL AND price >= ?")
            params.append(min_price)

        min_profit = self._safe_float(self.min_profit_filter_var.get().strip())
        max_profit = self._safe_float(self.max_profit_filter_var.get().strip())
        if min_profit is not None and max_profit is not None and max_profit < min_profit:
            min_profit, max_profit = max_profit, min_profit
        if min_profit is not None:
            where_clauses.append("potential_profit IS NOT NULL AND potential_profit >= ?")
            params.append(min_profit)
        if max_profit is not None:
            where_clauses.append("potential_profit IS NOT NULL AND potential_profit <= ?")
            params.append(max_profit)

        if where_clauses:
            query += " WHERE " + " AND ".join(where_clauses)
        
        query += " ORDER BY potential_profit DESC"
        
        cursor.execute(query, tuple(params))
        rows = cursor.fetchall()
        conn.close()
        
        # Add to tree
        for row in rows:
            listing_id, title, price, model, condition, max_buy, profit, status, score, user_flag, opened_at = row
            score_str = f"{score:.0f}%" if score is not None else "N/A"
            
            # Format values
            price_str = f"${price:.0f}" if price is not None else "N/A"
            max_buy_str = f"${max_buy:.0f}" if max_buy is not None and max_buy > 0 else "N/A"
            profit_str = f"${profit:.0f}" if profit is not None else "N/A"
            
            row_tags = ()
            if user_flag == "scam":
                row_tags = ("flag_scam",)
            elif user_flag == "interested":
                row_tags = ("flag_interested",)
            elif user_flag == "not_interested":
                row_tags = ("flag_not_interested",)
            elif opened_at:
                row_tags = ("flag_opened",)

            self.listings_tree.insert(
                "",
                tk.END,
                values=(listing_id, model, price_str, max_buy_str, profit_str, condition, score_str, status),
                tags=row_tags,
            )
        desktop_sync.restore_treeview_listing_state(self.listings_tree, selection_state)
        
        self.status_bar.config(text=f"Loaded {len(rows)} listings")
        
    def on_listing_double_click(self, event):
        """Handle double-click on a listing."""
        self.open_selected_listing()
        
    def show_listing_context_menu(self, event):
        """Show context menu for listings."""
        row_id = self.listings_tree.identify_row(event.y)
        current_selection = set(self.listings_tree.selection())
        if row_id and row_id not in current_selection:
            self.listings_tree.selection_set(row_id)
        if row_id:
            self.listings_tree.focus(row_id)

        if not self.listings_tree.selection():
            return
        try:
            self.listing_context_menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.listing_context_menu.grab_release()

    def set_selected_listing_flag(self, flag: str):
        """Set a visual flag for the currently selected listing."""
        if flag not in {"scam", "interested", "not_interested"}:
            return

        listing_ids = self._get_selected_listing_ids()
        if not listing_ids:
            messagebox.showwarning("No Selection", "Please select one or more listings first.")
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        now_iso = datetime.now().isoformat()
        cursor.executemany(
            "UPDATE listings SET user_flag = ?, updated_at = ? WHERE id = ?",
            [(flag, now_iso, listing_id) for listing_id in listing_ids],
        )
        conn.commit()
        conn.close()

        self.refresh_listings()
        self.status_bar.config(text=f"Marked {len(listing_ids)} listing(s) as {flag}")

    def clear_selected_listing_flag(self):
        """Clear any visual flag for the selected listing."""
        listing_ids = self._get_selected_listing_ids()
        if not listing_ids:
            messagebox.showwarning("No Selection", "Please select one or more listings first.")
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        now_iso = datetime.now().isoformat()
        cursor.executemany(
            "UPDATE listings SET user_flag = NULL, updated_at = ? WHERE id = ?",
            [(now_iso, listing_id) for listing_id in listing_ids],
        )
        conn.commit()
        conn.close()

        self.refresh_listings()
        self.status_bar.config(text=f"Cleared marking for {len(listing_ids)} listing(s)")

    def delete_selected_listings(self):
        """Delete one or more selected listings from local database."""
        listing_ids = self._get_selected_listing_ids()
        if not listing_ids:
            messagebox.showwarning("No Selection", "Please select one or more listings first.")
            return

        count = len(listing_ids)
        prompt = (
            f"Delete {count} selected listing(s)?\n\n"
            "This removes them from your local app database."
        )
        if not messagebox.askyesno("Delete Listings", prompt):
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.executemany("DELETE FROM listings WHERE id = ?", [(listing_id,) for listing_id in listing_ids])
        cursor.executemany("DELETE FROM conversations WHERE listing_id = ?", [(listing_id,) for listing_id in listing_ids])
        conn.commit()
        conn.close()

        self.refresh_listings()
        self.refresh_negotiation_listings()
        self.status_bar.config(text=f"Deleted {count} listing(s)")
        
    def open_selected_listing(self):
        """Open the selected listing in browser."""
        listing_id = self._get_active_listing_id()
        if not listing_id:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return

        canonical_url = f"https://www.facebook.com/marketplace/item/{listing_id}/"
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE listings SET url = ?, updated_at = ? WHERE id = ?",
            (canonical_url, datetime.now().isoformat(), listing_id),
        )
        conn.commit()
        conn.close()

        webbrowser.open(canonical_url)
        self._mark_listing_as_opened(listing_id)
        self.refresh_listings()
        self.status_bar.config(text=f"Opened listing {listing_id} in browser")
            
    def start_negotiation_from_listing(self):
        """Start negotiation for selected listing."""
        listing_id = self._get_active_listing_id()
        if not listing_id:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return
        
        # Switch to negotiation tab
        self.notebook.select(1)
        
        # Load the listing in negotiation tab
        self.refresh_negotiation_listings()
        values = self.negotiation_listing_combo["values"]
        for idx, value in enumerate(values):
            if value.startswith(f"{listing_id} - "):
                self.negotiation_listing_combo.current(idx)
                self.load_conversation(None)
                break
        
    def mark_as_purchased(self):
        """Mark selected listing as purchased."""
        listing_id = self._get_active_listing_id()
        if not listing_id:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return
        
        # Ask for purchase price
        price = simpledialog.askfloat(
            "Purchase Price",
            "Enter the final purchase price:",
            parent=self.root
        )
        
        if price is not None:
            result = deal_tracker.mark_as_purchased(listing_id, price)
            if result.get("success"):
                messagebox.showinfo("Success", f"Listing {listing_id} marked as purchased for ${price:.2f}")
                self.refresh_listings()
                self.refresh_deals()
            else:
                messagebox.showerror("Error", result.get("error", "Failed to record purchase"))
            
    # ===== Negotiation Tab Functions =====
    
    def refresh_negotiation_listings(self):
        """Refresh the negotiation listings dropdown."""
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT id, title, model FROM listings WHERE status != 'purchased' ORDER BY created_at DESC")
        rows = cursor.fetchall()
        conn.close()
        
        listings = [f"{row[0]} - {row[2]} - {row[1][:50]}" for row in rows]
        self.negotiation_listing_combo["values"] = listings
        
        if listings:
            self.negotiation_listing_combo.current(0)
            self.load_conversation(None)
            
    def load_conversation(self, event):
        """Load conversation history for selected listing."""
        selected = self.negotiation_listing_var.get()
        if not selected:
            return
        
        listing_id = selected.split(" - ")[0]
        
        # Clear conversation
        self.conversation_text.delete(1.0, tk.END)
        
        # Load from database
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT message_type, message_text, timestamp
            FROM conversations
            WHERE listing_id = ?
            ORDER BY timestamp ASC
        """, (listing_id,))
        rows = cursor.fetchall()
        conn.close()
        
        if not rows:
            self.conversation_text.insert(tk.END, "No conversation history yet.\n\n", "system")
            self.conversation_text.insert(tk.END, "Click 'Generate AI Response' to create an initial message.", "system")
        else:
            for msg_type, msg_text, timestamp in rows:
                if msg_type == "user":
                    self.conversation_text.insert(tk.END, "Seller: ", "seller")
                else:
                    self.conversation_text.insert(tk.END, "You: ", "you")
                
                self.conversation_text.insert(tk.END, f"{msg_text}\n\n")
        
        self.conversation_text.see(tk.END)
        
    def generate_ai_response(self):
        """Generate AI response for the conversation."""
        selected = self.negotiation_listing_var.get()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return
        
        listing_id = selected.split(" - ")[0]
        
        # Get seller's message from input
        seller_message = self.message_input.get(1.0, tk.END).strip()
        
        if not seller_message:
            # Generate initial message
            self.status_bar.config(text="Generating initial message...")
            self.root.update()
            
            try:
                response = generate_initial_message(listing_id)
                self.message_input.delete(1.0, tk.END)
                self.message_input.insert(1.0, response)
                self.status_bar.config(text="Initial message generated")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to generate message: {str(e)}")
                self.status_bar.config(text="Error generating message")
        else:
            # Generate response
            self.status_bar.config(text="Generating AI response...")
            self.root.update()
            
            try:
                response = generate_response(listing_id, seller_message)
                
                # Add to conversation display
                self.conversation_text.insert(tk.END, "Seller: ", "seller")
                self.conversation_text.insert(tk.END, f"{seller_message}\n\n")
                self.conversation_text.insert(tk.END, "You: ", "you")
                self.conversation_text.insert(tk.END, f"{response}\n\n")
                self.conversation_text.see(tk.END)
                
                # Clear input and show response
                self.message_input.delete(1.0, tk.END)
                self.message_input.insert(1.0, response)
                
                self.status_bar.config(text="AI response generated")
            except Exception as e:
                messagebox.showerror("Error", f"Failed to generate response: {str(e)}")
                self.status_bar.config(text="Error generating response")
                
    def copy_message_to_clipboard(self):
        """Copy the message to clipboard."""
        message = self.message_input.get(1.0, tk.END).strip()
        if message:
            self.root.clipboard_clear()
            self.root.clipboard_append(message)
            self.status_bar.config(text="Message copied to clipboard")
        else:
            messagebox.showwarning("Empty Message", "No message to copy.")
            
    def analyze_current_conversation(self):
        """Analyze the current conversation."""
        selected = self.negotiation_listing_var.get()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return
        
        listing_id = selected.split(" - ")[0]
        
        self.status_bar.config(text="Analyzing conversation...")
        self.root.update()
        
        try:
            analysis = analyze_conversation(listing_id)
            
            # Show in a new window
            analysis_window = tk.Toplevel(self.root)
            analysis_window.title("Conversation Analysis")
            analysis_window.geometry("600x400")
            
            text = scrolledtext.ScrolledText(analysis_window, wrap=tk.WORD, font=("Arial", 10))
            text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
            text.insert(1.0, analysis)
            text.config(state=tk.DISABLED)
            
            self.status_bar.config(text="Analysis complete")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to analyze: {str(e)}")
            self.status_bar.config(text="Error analyzing conversation")
            
    def mark_as_purchased_from_negotiation(self):
        """Mark current listing as purchased from negotiation tab."""
        selected = self.negotiation_listing_var.get()
        if not selected:
            messagebox.showwarning("No Selection", "Please select a listing first.")
            return
        
        listing_id = selected.split(" - ")[0]
        
        # Ask for purchase price
        price = simpledialog.askfloat(
            "Purchase Price",
            "Enter the final purchase price:",
            parent=self.root
        )
        
        if price is not None:
            result = deal_tracker.mark_as_purchased(listing_id, price)
            if result.get("success"):
                messagebox.showinfo("Success", f"Listing {listing_id} marked as purchased for ${price:.2f}")
                self.refresh_negotiation_listings()
                self.refresh_deals()
            else:
                messagebox.showerror("Error", result.get("error", "Failed to record purchase"))
            
    # ===== Deals Tab Functions =====
    
    def refresh_deals(self):
        """Refresh the deals table."""
        # Clear existing items
        for item in self.deals_tree.get_children():
            self.deals_tree.delete(item)
        
        # Get purchase history
        history = deal_tracker.get_purchase_history()
        
        total_profit = 0
        total_spent = 0
        
        for deal in history:
            date = deal.get("purchase_date", "N/A")
            model = deal.get("model", "N/A")
            bought_for = deal.get("final_price", 0)
            sold_for = deal.get("resale_price_actual", 0) or 0
            repair_cost = deal.get("repair_costs_actual", 0) or 0
            actual_profit = deal.get("actual_profit")

            if actual_profit is not None:
                profit = actual_profit
                status = "Sold"
            elif sold_for > 0:
                profit = sold_for - bought_for - repair_cost
                status = "Sold"
            else:
                profit = 0
                status = "Purchased"
            
            total_profit += profit
            total_spent += bought_for
            
            self.deals_tree.insert(
                "",
                tk.END,
                values=(
                    date[:10] if date != "N/A" else "N/A",
                    model,
                    f"${bought_for:.0f}",
                    f"${sold_for:.0f}" if sold_for > 0 else "N/A",
                    f"${repair_cost:.0f}" if repair_cost > 0 else "N/A",
                    f"${profit:.0f}",
                    status
                )
            )
        
        # Update summary
        if history:
            summary_text = f"Total Purchases: {len(history)} | Total Spent: ${total_spent:.2f} | Total Profit: ${total_profit:.2f}"
        else:
            summary_text = "No purchases yet"
        
        self.summary_label.config(text=summary_text)
        
    # ===== Menu Functions =====

    def _format_elapsed(self, total_seconds: int) -> str:
        """Format elapsed seconds as mm:ss or hh:mm:ss."""
        hours, remainder = divmod(max(total_seconds, 0), 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        return f"{minutes:02d}:{seconds:02d}"

    def _set_scraper_controls(self, running: bool):
        """Enable/disable scraper action controls."""
        server_mode = self._is_server_sync_enabled()
        if hasattr(self, "run_scraper_button"):
            if server_mode:
                self.run_scraper_button.config(state=tk.DISABLED)
            else:
                self.run_scraper_button.config(state=tk.DISABLED if running else tk.NORMAL)
        if hasattr(self, "cancel_scraper_button"):
            self.cancel_scraper_button.config(state=tk.NORMAL if (running and not server_mode) else tk.DISABLED)
    
    def run_scraper(self):
        """Run the scraper in a separate thread."""
        if self._is_server_sync_enabled():
            messagebox.showinfo(
                "Server Sync Mode",
                "Local scraper is disabled while server sync is enabled.\n"
                "Use VPS workers and keep this app as your live viewer.",
            )
            return

        if self.scraper_thread and self.scraper_thread.is_alive():
            messagebox.showinfo("Scraper Running", "A scraper job is already running.")
            return

        account_ctx, account_error = self._reserve_next_scraper_account_for_run()
        if account_error:
            self.status_bar.config(text=account_error)
            messagebox.showerror("Scraper Account Required", account_error)
            return

        account_label = (
            account_ctx.get("account_name")
            or account_ctx.get("email")
            or f"Account {account_ctx['account_id']}"
        )

        self.scraper_started_at = datetime.now()
        self.scraper_stop_event = threading.Event()
        self.scraper_result_data = {
            "returncode": None,
            "error": None,
            "new_count": 0,
            "cancelled": False,
            "account_label": account_label,
            "current_query": None,
            "query_index": 0,
            "query_total": 0,
            "last_found": None,
        }

        def on_progress(payload):
            event = payload.get("event")
            if event == "query_start":
                self.scraper_result_data["current_query"] = payload.get("query")
                self.scraper_result_data["query_index"] = payload.get("query_index", 0)
                self.scraper_result_data["query_total"] = payload.get("query_total", 0)
                self.scraper_result_data["last_found"] = None
            elif event == "query_result":
                self.scraper_result_data["current_query"] = payload.get("query")
                self.scraper_result_data["query_index"] = payload.get("query_index", 0)
                self.scraper_result_data["query_total"] = payload.get("query_total", 0)
                self.scraper_result_data["last_found"] = payload.get("found")
            elif event == "query_error":
                self.scraper_result_data["current_query"] = payload.get("query")
                self.scraper_result_data["query_index"] = payload.get("query_index", 0)
                self.scraper_result_data["query_total"] = payload.get("query_total", 0)
                self.scraper_result_data["last_found"] = "error"
            elif event == "listing_saved":
                if "new_count" in payload:
                    self.scraper_result_data["new_count"] = payload.get("new_count", 0)
                else:
                    self.scraper_result_data["new_count"] = self.scraper_result_data.get("new_count", 0) + 1
            elif event == "cancelled":
                self.scraper_result_data["cancelled"] = True
            elif event == "completed":
                self.scraper_result_data["new_count"] = payload.get("new_count", 0)
                self.scraper_result_data["cancelled"] = payload.get("cancelled", False)
                self.scraper_result_data["query_index"] = payload.get(
                    "query_index", self.scraper_result_data.get("query_index", 0)
                )
                self.scraper_result_data["query_total"] = payload.get(
                    "query_total", self.scraper_result_data.get("query_total", 0)
                )

        def scrape():
            bridge_process = None
            account_id = int(account_ctx["account_id"])
            try:
                profile_path = (account_ctx.get("profile_path") or "").strip()
                if profile_path:
                    user_data_dir = profile_path
                else:
                    user_data_dir = str(Path(__file__).parent / "browser_profiles" / f"fb_account_{account_ctx['account_id']}")

                proxy_config, bridge_process = self._build_playwright_proxy_for_account(account_ctx)
                if not proxy_config:
                    raise RuntimeError(
                        "Selected scraper account has no usable SOCKS5 proxy. "
                        "Assign a valid ACTIVE SOCKS5 proxy and try again."
                    )

                # Track selected account usage in DB.
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE fb_accounts SET status = 'ACTIVE', cooldown_until = NULL, updated_at = ? WHERE id = ?",
                    (datetime.now().isoformat(), account_ctx["account_id"]),
                )
                conn.commit()
                conn.close()

                new_listings = asyncio.run(
                    scrape_marketplace(
                        headless=True,
                        progress_callback=on_progress,
                        stop_event=self.scraper_stop_event,
                        user_data_dir=user_data_dir,
                        proxy=proxy_config,
                    )
                )
                update_fb_account_runtime_status(account_id, success=True)
                if new_listings:
                    deal_tracker.update_listing_conversion_scores()
                    notify_only_raw = self._get_scraper_setting("notify_profitable_only", "1").strip().lower()
                    notify_profitable_only = notify_only_raw in {"1", "true", "yes", "on"}
                    if notify_profitable_only:
                        notifiable_listings = [
                            listing for listing in new_listings
                            if (listing.get("potential_profit") or 0) >= 0
                        ]
                    else:
                        notifiable_listings = list(new_listings)
                    if notifiable_listings:
                        notify_new_listings(notifiable_listings)
                self.scraper_result_data["returncode"] = 0
                self.scraper_result_data["new_count"] = len(new_listings or [])
            except Exception as e:
                update_fb_account_runtime_status(account_id, success=False, reason=str(e))
                self.scraper_result_data["error"] = str(e)
            finally:
                if bridge_process and bridge_process.poll() is None:
                    try:
                        bridge_process.terminate()
                        bridge_process.wait(timeout=3)
                    except Exception:
                        try:
                            bridge_process.kill()
                        except Exception:
                            pass

        def check_completion():
            if self.scraper_thread and self.scraper_thread.is_alive():
                elapsed = int((datetime.now() - self.scraper_started_at).total_seconds())
                elapsed_str = self._format_elapsed(elapsed)
                query_index = self.scraper_result_data.get("query_index", 0)
                query_total = self.scraper_result_data.get("query_total", 0)
                current_query = self.scraper_result_data.get("current_query")
                last_found = self.scraper_result_data.get("last_found")

                if current_query:
                    progress_text = f"{query_index}/{query_total} - {current_query}"
                    if isinstance(last_found, int):
                        progress_text += f" ({last_found} found)"
                    elif last_found == "error":
                        progress_text += " (error)"
                else:
                    progress_text = "starting..."

                action = "Stopping scraper" if self.scraper_stop_event.is_set() else "Running scraper"
                account_label = self.scraper_result_data.get("account_label", "unknown")
                self.status_bar.config(text=f"{action} [{elapsed_str}] | {account_label} | {progress_text}")
                self.root.after(500, check_completion)
                return

            self._set_scraper_controls(False)

            if self.scraper_result_data["error"]:
                self.status_bar.config(text=f"Scraper error: {self.scraper_result_data['error']}")
                return

            if self.scraper_result_data["returncode"] == 0:
                self.refresh_listings()
                query_index = self.scraper_result_data.get("query_index", 0)
                query_total = self.scraper_result_data.get("query_total", 0)
                new_count = self.scraper_result_data.get("new_count", 0)

                if self.scraper_result_data.get("cancelled"):
                    self.status_bar.config(
                        text=(
                            f"Scraper cancelled [{self.scraper_result_data.get('account_label', 'unknown')}] "
                            f"({new_count} new, {query_index}/{query_total} queries)"
                        )
                    )
                else:
                    self.status_bar.config(
                        text=(
                            f"Scraper completed [{self.scraper_result_data.get('account_label', 'unknown')}] "
                            f"({new_count} new, {query_index}/{query_total} queries)"
                        )
                    )
            else:
                self.status_bar.config(
                    text=f"Scraper error: exit code {self.scraper_result_data['returncode']}"
                )

        self._set_scraper_controls(True)
        self.status_bar.config(text=f"Running scraper [00:00] | {account_label} | starting...")
        self.scraper_thread = threading.Thread(target=scrape, daemon=True, name="scraper-worker")
        self.scraper_thread.start()
        self.root.after(500, check_completion)

    def cancel_scraper(self):
        """Request cancellation of the active scraper job."""
        if not self.scraper_thread or not self.scraper_thread.is_alive():
            self.status_bar.config(text="No active scraper to stop.")
            return

        if self.scraper_stop_event and not self.scraper_stop_event.is_set():
            self.scraper_stop_event.set()
            self.status_bar.config(text="Stopping scraper after current query...")
            self.cancel_scraper_button.config(state=tk.DISABLED)
        
    def start_monitor(self):
        """Start the monitor in a separate process."""
        if self._is_server_sync_enabled():
            messagebox.showinfo(
                "Server Sync Mode",
                "Local monitor is disabled while server sync is enabled.\n"
                "Your VPS workers are the active scrapers.",
            )
            return

        try:
            interval = self._get_scraper_setting("monitor_interval_minutes", "30")
            subprocess.Popen(
                [sys.executable, "main.py", "monitor", "--interval", str(interval)],
                cwd=Path(__file__).parent,
            )
            messagebox.showinfo(
                "Monitor Started",
                f"Background monitor started. It will check for new listings every {interval} minutes.",
            )
        except Exception as e:
            messagebox.showerror("Error", f"Failed to start monitor: {str(e)}")
            
    def show_patterns(self):
        """Show learned patterns."""
        patterns = deal_tracker.analyze_patterns()
        
        # Show in a new window
        window = tk.Toplevel(self.root)
        window.title("Learned Patterns")
        window.geometry("700x500")
        
        text = scrolledtext.ScrolledText(window, wrap=tk.WORD, font=("Arial", 10))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        if patterns:
            text.insert(1.0, "=== LEARNED PATTERNS ===\n\n")
            text.insert(tk.END, json.dumps(patterns, indent=2))
        else:
            text.insert(1.0, "No patterns learned yet. Start recording purchases to see patterns!")
        
        text.config(state=tk.DISABLED)
        
    def show_history(self):
        """Show purchase history in detail."""
        # Just switch to deals tab
        self.notebook.select(2)
        self.refresh_deals()
        
    def show_insights(self):
        """Show actionable insights."""
        insights = deal_tracker.get_insights()
        
        # Show in a new window
        window = tk.Toplevel(self.root)
        window.title("Insights & Recommendations")
        window.geometry("700x500")
        
        text = scrolledtext.ScrolledText(window, wrap=tk.WORD, font=("Arial", 10))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        if insights:
            text.insert(1.0, "=== INSIGHTS & RECOMMENDATIONS ===\n\n")
            summary = insights.get("summary", {})
            text.insert(
                tk.END,
                f"Summary\n"
                f"- Total purchases: {summary.get('total_purchases', 0)}\n"
                f"- Conversion rate: {summary.get('conversion_rate', 0)}%\n"
                f"- Avg discount: {summary.get('avg_discount', 0)}%\n"
                f"- Avg time to close: {summary.get('avg_time_to_close', 0)} hours\n\n"
            )

            best_models = insights.get("best_models", [])
            if best_models:
                text.insert(tk.END, "Best Models\n")
                for item in best_models:
                    text.insert(
                        tk.END,
                        f"- {item['model']}: {item['avg_discount']}% avg discount ({item['purchases']} purchases)\n"
                    )
                text.insert(tk.END, "\n")

            best_keywords = insights.get("best_keywords", [])
            if best_keywords:
                text.insert(tk.END, "Best Keywords\n")
                for item in best_keywords:
                    text.insert(
                        tk.END,
                        f"- {item['keyword']}: {item['avg_discount']}% avg discount ({item['purchases']} purchases)\n"
                    )
                text.insert(tk.END, "\n")

            recommendations = insights.get("recommendations", [])
            text.insert(tk.END, "Recommendations\n")
            if recommendations:
                for rec in recommendations:
                    text.insert(tk.END, f"- {rec}\n")
            else:
                text.insert(tk.END, "- Keep recording purchases to improve recommendations.\n")
        else:
            text.insert(1.0, "No insights yet. Record more purchases to get personalized recommendations!")
        
        text.config(state=tk.DISABLED)

    # ===== Settings Manager Functions =====

    def _get_scraper_setting(self, key: str, default: str = "") -> str:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT setting_value FROM scraper_settings WHERE setting_key = ?", (key,))
        row = cursor.fetchone()
        conn.close()
        return str(row[0]) if row and row[0] is not None else str(default)

    def _set_scraper_setting(self, key: str, value: str):
        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
            VALUES (?, ?, ?)
            ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
            """,
            (key, str(value), now_iso),
        )
        conn.commit()
        conn.close()

    def _refresh_active_scraper_account_label(self):
        if not self.active_scraper_account_var:
            return
        active_raw = self._get_scraper_setting("active_scraper_account_id", "").strip()
        preferred_ctx = self._get_active_scraper_account_context()
        eligible_count = len(self._list_eligible_scraper_account_contexts())

        if preferred_ctx:
            preferred_label = (
                preferred_ctx.get("account_name")
                or preferred_ctx.get("email")
                or f"Account {preferred_ctx['account_id']}"
            )
        elif active_raw:
            preferred_label = f"ID {active_raw} (missing)"
        else:
            preferred_label = "auto (lowest eligible ID)"

        self.active_scraper_account_var.set(
            f"Rotation start account: {preferred_label} | Eligible ACTIVE SOCKS5 accounts: {eligible_count}"
        )

    def _load_proxy_options(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT id, COALESCE(name, ''), host, port
            FROM proxies
            ORDER BY COALESCE(name, host), host
            """
        )
        rows = cursor.fetchall()
        conn.close()

        options = {"None": None}
        for proxy_id, name, host, port in rows:
            label = f"{proxy_id} - {(name or host)} ({host}:{port})"
            options[label] = proxy_id
        self.proxy_options_by_id = options
        return list(options.keys())

    def _refresh_proxy_dropdown(self):
        if "proxy_label" not in self.account_form_vars:
            return
        combo = self.account_form_vars["proxy_label_widget"]
        values = self._load_proxy_options()
        combo["values"] = values
        current = self.account_form_vars["proxy_label"].get()
        if current not in values:
            self.account_form_vars["proxy_label"].set("None")

    def _refresh_account_tree(self):
        if not self.account_tree:
            return
        for item in self.account_tree.get_children():
            self.account_tree.delete(item)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, COALESCE(a.account_name, ''), COALESCE(a.email, ''), COALESCE(a.status, ''),
                   COALESCE(p.name, p.host || ':' || p.port, ''), COALESCE(a.profile_path, '')
            FROM fb_accounts a
            LEFT JOIN proxies p ON p.id = a.proxy_id
            ORDER BY a.id DESC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            self.account_tree.insert("", tk.END, iid=str(row[0]), values=row)

    @staticmethod
    def _proxy_stats_key(proxy_type: str, host: str, port, username: str | None = None) -> str:
        scheme = str(proxy_type or "socks5").strip().lower() or "socks5"
        host_value = str(host or "").strip().lower()
        user_value = str(username or "").strip()
        try:
            port_value = int(port)
        except (TypeError, ValueError):
            return ""
        if not host_value or port_value <= 0:
            return ""
        return f"{scheme}:{host_value}:{port_value}:{user_value}"

    def _fetch_server_proxy_stats_map(self) -> tuple[dict[str, dict], str | None]:
        ctx, error = self._get_server_api_context()
        if error:
            return {}, error

        try:
            response = requests.get(
                f"{ctx['base_url']}/proxy-stats",
                headers=ctx["headers"],
                timeout=(4, 8),
            )
            if response.status_code == 401:
                return {}, "Unauthorized for proxy stats (check Server API Token)."
            if response.status_code == 404:
                return {}, "Server API does not expose /proxy-stats yet. Deploy latest API container."
            response.raise_for_status()
            payload = response.json() if response.content else {}
            items = payload.get("items") or []
        except Exception as exc:
            return {}, f"Failed to fetch proxy stats: {exc}"

        stats_by_key: dict[str, dict] = {}
        for item in items:
            key = str((item or {}).get("proxy_key") or "").strip()
            if key:
                stats_by_key[key] = item
        return stats_by_key, None

    def _refresh_proxy_tree(self, show_proxy_stats_error: bool = False, preserve_selection: bool = True):
        if not self.proxy_tree:
            return
        selected_proxy_iid = str(self.selected_proxy_id) if preserve_selection and self.selected_proxy_id else None
        for item in self.proxy_tree.get_children():
            self.proxy_tree.delete(item)

        proxy_stats_by_key: dict[str, dict] = {}
        proxy_stats_error = None
        if requests is not None:
            proxy_stats_by_key, proxy_stats_error = self._fetch_server_proxy_stats_map()
        if proxy_stats_error and show_proxy_stats_error:
            messagebox.showwarning("Proxy Stats", proxy_stats_error)
            self.status_bar.config(text=proxy_stats_error)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT p.id, COALESCE(p.name, ''), COALESCE(p.proxy_type, 'http'),
                   COALESCE(p.host, ''), p.port, COALESCE(p.username, ''),
                   COALESCE(p.status, ''), COALESCE(p.country, ''),
                   COUNT(a.id) AS assigned_count
            FROM proxies p
            LEFT JOIN fb_accounts a ON a.proxy_id = p.id
            GROUP BY p.id
            ORDER BY p.id DESC
            """
        )
        rows = cursor.fetchall()
        conn.close()

        for row in rows:
            proxy_id = row[0]
            proxy_type = str(row[2] or "http")
            host = str(row[3] or "")
            port = row[4]
            username = str(row[5] or "")
            proxy_status = str(row[6] or "")
            country = str(row[7] or "")
            assigned_count = row[8]
            endpoint = f"{host}:{port}" if host and port else ""

            proxy_key = self._proxy_stats_key(proxy_type=proxy_type, host=host, port=port, username=username)
            stats = proxy_stats_by_key.get(proxy_key) or {}
            consecutive_failures = max(0, self._safe_int(stats.get("consecutive_failures"), 0))
            is_banned_raw = stats.get("is_banned")
            if isinstance(is_banned_raw, bool):
                is_banned = is_banned_raw
            else:
                is_banned = str(is_banned_raw or "").strip().lower() in {"1", "true", "yes", "on"}
            banned_until = str(stats.get("banned_until") or "").strip()
            banned_until_display = banned_until.replace("T", " ")[:19] if banned_until else "-"
            values = (
                proxy_id,
                row[1],
                proxy_type,
                endpoint,
                proxy_status,
                country,
                assigned_count,
                str(consecutive_failures),
                "yes" if is_banned else "no",
                banned_until_display,
            )
            self.proxy_tree.insert("", tk.END, iid=str(row[0]), values=values)

        if selected_proxy_iid and selected_proxy_iid in self.proxy_tree.get_children():
            self.proxy_tree.selection_set(selected_proxy_iid)
            self.proxy_tree.focus(selected_proxy_iid)
            self.proxy_tree.see(selected_proxy_iid)

    def _clear_account_form(self):
        self.selected_account_id = None
        for key, var in self.account_form_vars.items():
            if key.endswith("_widget"):
                continue
            if isinstance(var, tk.StringVar):
                var.set("")
        self.account_form_vars["status"].set("ACTIVE")
        self.account_form_vars["proxy_label"].set("None")
        if self.account_tree:
            self.account_tree.selection_remove(self.account_tree.selection())

    def _clear_proxy_form(self):
        self.selected_proxy_id = None
        for key, var in self.proxy_form_vars.items():
            if isinstance(var, tk.StringVar):
                var.set("")
        self.proxy_form_vars["proxy_type"].set("http")
        self.proxy_form_vars["status"].set("active")
        if self.proxy_tree:
            self.proxy_tree.selection_remove(self.proxy_tree.selection())

    def _on_account_selected(self, event=None):
        if not self.account_tree:
            return
        selection = self.account_tree.selection()
        if not selection:
            return
        account_id = int(selection[0])

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT account_name, email, status, profile_path, user_agent, proxy_id, notes
            FROM fb_accounts
            WHERE id = ?
            """,
            (account_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return

        self.selected_account_id = account_id
        self.account_form_vars["account_name"].set(row[0] or "")
        self.account_form_vars["email"].set(row[1] or "")
        self.account_form_vars["status"].set(row[2] or "ACTIVE")
        self.account_form_vars["profile_path"].set(row[3] or "")
        self.account_form_vars["user_agent"].set(row[4] or "")
        proxy_id = row[5]
        proxy_label = "None"
        for label, option_id in self.proxy_options_by_id.items():
            if option_id == proxy_id:
                proxy_label = label
                break
        self.account_form_vars["proxy_label"].set(proxy_label)
        self.account_form_vars["notes"].set(row[6] or "")

    def _on_proxy_selected(self, event=None):
        if not self.proxy_tree:
            return
        selection = self.proxy_tree.selection()
        if not selection:
            return
        proxy_id = int(selection[0])

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT name, proxy_type, host, port, username, password, country, status, notes
            FROM proxies
            WHERE id = ?
            """,
            (proxy_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return

        self.selected_proxy_id = proxy_id
        self.proxy_form_vars["name"].set(row[0] or "")
        self.proxy_form_vars["proxy_type"].set(row[1] or "http")
        self.proxy_form_vars["host"].set(row[2] or "")
        self.proxy_form_vars["port"].set(str(row[3] or ""))
        self.proxy_form_vars["username"].set(row[4] or "")
        self.proxy_form_vars["password"].set(row[5] or "")
        self.proxy_form_vars["country"].set(row[6] or "")
        self.proxy_form_vars["status"].set(row[7] or "active")
        self.proxy_form_vars["notes"].set(row[8] or "")

    def _save_proxy(self):
        host = self.proxy_form_vars["host"].get().strip()
        port_raw = self.proxy_form_vars["port"].get().strip()
        if not host:
            messagebox.showwarning("Validation Error", "Proxy host is required.")
            return
        try:
            port = int(port_raw)
        except ValueError:
            messagebox.showwarning("Validation Error", "Proxy port must be a number.")
            return

        payload = (
            self.proxy_form_vars["name"].get().strip() or None,
            self.proxy_form_vars["proxy_type"].get().strip() or "http",
            host,
            port,
            self.proxy_form_vars["username"].get().strip() or None,
            self.proxy_form_vars["password"].get().strip() or None,
            self.proxy_form_vars["country"].get().strip() or None,
            self.proxy_form_vars["status"].get().strip() or "active",
            self.proxy_form_vars["notes"].get().strip() or None,
            datetime.now().isoformat(),
        )

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        try:
            if self.selected_proxy_id:
                cursor.execute(
                    """
                    UPDATE proxies
                    SET name=?, proxy_type=?, host=?, port=?, username=?, password=?, country=?, status=?, notes=?, updated_at=?
                    WHERE id=?
                    """,
                    payload + (self.selected_proxy_id,),
                )
            else:
                cursor.execute(
                    """
                    INSERT INTO proxies (name, proxy_type, host, port, username, password, country, status, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload + (datetime.now().isoformat(),),
                )
            conn.commit()
        except sqlite3.IntegrityError:
            messagebox.showerror("Save Failed", "A proxy with the same host/port/username already exists.")
            conn.close()
            return
        conn.close()

        self._refresh_proxy_tree()
        self._refresh_proxy_dropdown()
        self._clear_proxy_form()
        self.status_bar.config(text="Proxy settings saved")

    def _reset_selected_proxy_ban(self):
        if not self.selected_proxy_id:
            messagebox.showwarning("No Selection", "Select a proxy first.")
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT COALESCE(proxy_type, 'socks5'), COALESCE(host, ''), port, COALESCE(username, '')
            FROM proxies
            WHERE id = ?
            """,
            (self.selected_proxy_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            messagebox.showwarning("Proxy Not Found", "The selected proxy record no longer exists.")
            return

        proxy_type = str(row[0] or "socks5").strip().lower() or "socks5"
        host = str(row[1] or "").strip()
        port = row[2]
        username = str(row[3] or "").strip()
        if not host or not port:
            messagebox.showwarning("Invalid Proxy", "Selected proxy has invalid host/port.")
            return

        ctx, error = self._get_server_api_context()
        if error:
            messagebox.showwarning("Proxy Ban Reset", error)
            return

        payload = {
            "proxy_server": f"{proxy_type}://{host}:{port}",
            "proxy_username": username or None,
            "clear_failures": True,
        }
        try:
            response = requests.post(
                f"{ctx['base_url']}/proxy-stats/reset",
                headers=ctx["headers"],
                json=payload,
                timeout=(8, 25),
            )
            if response.status_code == 401:
                raise RuntimeError("Unauthorized (check Server API Token).")
            if response.status_code == 404:
                raise RuntimeError("Server API does not expose /proxy-stats/reset yet. Deploy latest API container.")
            response.raise_for_status()
        except Exception as exc:
            messagebox.showerror("Proxy Ban Reset Failed", f"Failed to reset selected proxy ban:\n{exc}")
            self.status_bar.config(text="Failed to reset selected proxy ban")
            return

        self._refresh_proxy_tree(preserve_selection=True)
        self._on_proxy_selected()
        self.status_bar.config(text=f"Reset proxy ban for {host}:{port}")

    def _parse_proxy_from_string(self, value: str, default_type: str = "http", default_country: str = ""):
        raw = (value or "").strip()
        if not raw:
            return None

        # Accept raw cURL integration examples and extract the -x proxy endpoint.
        if raw.lower().startswith("curl "):
            match = re.search(r"(?:^|\s)-x\s+([^\s]+)", raw)
            if not match:
                return None
            return self._parse_proxy_from_string(
                match.group(1).strip("'\""),
                default_type=default_type,
                default_country=default_country,
            )

        if "://" in raw:
            parsed = urlparse(raw)
            if not parsed.hostname or not parsed.port:
                return None
            return {
                "name": parsed.hostname,
                "proxy_type": (parsed.scheme or default_type or "http"),
                "host": parsed.hostname,
                "port": int(parsed.port),
                "username": parsed.username,
                "password": parsed.password,
                "country": default_country or None,
                "status": "active",
                "notes": "Imported from API",
            }

        parts = [p.strip() for p in raw.split(":")]
        if len(parts) >= 2 and parts[1].isdigit():
            host = parts[0]
            port = int(parts[1])
            username = parts[2] if len(parts) >= 3 and parts[2] else None
            password = parts[3] if len(parts) >= 4 and parts[3] else None
            return {
                "name": host,
                "proxy_type": default_type or "http",
                "host": host,
                "port": port,
                "username": username,
                "password": password,
                "country": default_country or None,
                "status": "active",
                "notes": "Imported from API",
            }

        return None

    @staticmethod
    def _format_proxy_record_as_url(record: dict) -> str | None:
        host = str(record.get("host") or "").strip()
        if not host:
            return None
        try:
            port = int(str(record.get("port") or "").strip())
        except (TypeError, ValueError):
            return None
        if port <= 0:
            return None

        proxy_type = str(record.get("proxy_type") or "socks5").strip().lower() or "socks5"
        username = str(record.get("username") or "").strip()
        password = str(record.get("password") or "").strip()
        auth = ""
        if username or password:
            auth_user = quote(username, safe="")
            auth_password = quote(password, safe="")
            auth = f"{auth_user}:{auth_password}@"
        return f"{proxy_type}://{auth}{host}:{port}"

    def _parse_proxy_records_from_file(self, file_path: str, force_socks5: bool = False):
        path = Path(file_path)
        if not path.exists():
            return []

        records = []
        default_country = self.scraper_setting_vars.get("proxy_api_default_country", tk.StringVar(value="")).get().strip()
        default_type = "socks5" if force_socks5 else (
            self.scraper_setting_vars.get("proxy_api_default_type", tk.StringVar(value="http")).get().strip() or "http"
        )

        def _normalize(record: dict | None):
            if not record:
                return
            proxy_type = str(record.get("proxy_type") or default_type or "http").strip().lower() or "http"
            if force_socks5 and proxy_type != "socks5":
                return
            record["proxy_type"] = "socks5" if force_socks5 else proxy_type
            record["status"] = str(record.get("status") or "active").strip() or "active"
            record["country"] = str(record.get("country") or default_country or "").strip() or None
            record["notes"] = str(record.get("notes") or "Imported from file").strip() or "Imported from file"
            records.append(record)

        if path.suffix.lower() == ".csv":
            try:
                with path.open("r", encoding="utf-8-sig", newline="") as handle:
                    reader = csv.DictReader(handle)
                    for row in reader:
                        if not isinstance(row, dict):
                            continue
                        endpoint = (
                            row.get("proxy")
                            or row.get("proxy_server")
                            or row.get("endpoint")
                            or row.get("url")
                            or row.get("server")
                        )
                        if endpoint:
                            parsed = self._parse_proxy_from_string(
                                str(endpoint),
                                default_type=default_type,
                                default_country=default_country,
                            )
                            _normalize(parsed)
                            continue

                        host = str(row.get("host") or row.get("ip") or "").strip()
                        port = str(row.get("port") or "").strip()
                        if not host or not port.isdigit():
                            continue
                        parsed = {
                            "name": str(row.get("name") or f"{host}:{port}").strip(),
                            "proxy_type": str(row.get("proxy_type") or row.get("type") or default_type).strip().lower(),
                            "host": host,
                            "port": int(port),
                            "username": str(row.get("username") or row.get("user") or "").strip() or None,
                            "password": str(row.get("password") or row.get("pass") or "").strip() or None,
                            "country": str(row.get("country") or default_country).strip() or None,
                            "status": str(row.get("status") or "active").strip(),
                            "notes": str(row.get("notes") or "Imported from CSV").strip(),
                        }
                        _normalize(parsed)
            except Exception:
                records = []

        # Fallback / TXT parse for one-proxy-per-line files.
        if not records:
            try:
                with path.open("r", encoding="utf-8-sig") as handle:
                    for raw_line in handle:
                        line = raw_line.strip()
                        if not line or line.startswith("#"):
                            continue
                        parsed = self._parse_proxy_from_string(
                            line,
                            default_type=default_type,
                            default_country=default_country,
                        )
                        _normalize(parsed)
            except Exception:
                return []

        deduped = []
        seen = set()
        for record in records:
            key = (
                str(record.get("host") or "").strip().lower(),
                int(record.get("port") or 0),
                str(record.get("username") or "").strip(),
            )
            if not key[0] or key[1] <= 0 or key in seen:
                continue
            seen.add(key)
            deduped.append(record)
        return deduped

    def _import_proxies_from_file_prompt(self):
        file_path = filedialog.askopenfilename(
            title="Import SOCKS5 Proxies (TXT/CSV)",
            filetypes=[
                ("Proxy files", "*.txt *.csv"),
                ("Text files", "*.txt"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
            parent=self.settings_window or self.root,
        )
        if not file_path:
            return

        records = self._parse_proxy_records_from_file(file_path, force_socks5=True)
        if not records:
            messagebox.showwarning(
                "Nothing Imported",
                "No valid SOCKS5 proxies were found in the selected file.",
            )
            return

        created, updated = self._upsert_proxy_records(records)
        self._refresh_proxy_tree()
        self._refresh_proxy_dropdown()
        self.status_bar.config(text=f"File import complete: {created} created, {updated} updated")
        messagebox.showinfo(
            "Import Complete",
            f"Processed {len(records)} SOCKS5 proxy record(s).\nCreated: {created}\nUpdated: {updated}",
        )

    def _parse_socks5_proxy_pool_entries(self, raw: str):
        entries = []
        seen = set()
        for token in re.split(r"[\n,;]+", str(raw or "").strip()):
            item = token.strip()
            if not item:
                continue
            parsed = self._parse_proxy_from_string(item, default_type="socks5")
            if not parsed:
                continue
            proxy_type = str(parsed.get("proxy_type") or "").strip().lower()
            if proxy_type != "socks5":
                continue
            proxy_server = self._format_proxy_record_as_url(parsed)
            if not proxy_server:
                continue
            username = str(parsed.get("username") or "").strip()
            password = str(parsed.get("password") or "").strip()
            key = (proxy_server, username, password)
            if key in seen:
                continue
            seen.add(key)
            entries.append(
                {
                    "proxy_server": proxy_server,
                    "proxy_username": username or None,
                    "proxy_password": password or None,
                }
            )
        return entries

    def _collect_auto_rotation_proxy_pool(self):
        pool = []
        seen = set()

        for profile in self._load_local_proxy_profiles():
            parsed_profile = urlparse(profile.get("proxy_server") or "")
            if str(parsed_profile.scheme or "").strip().lower() != "socks5":
                continue
            record = {
                "proxy_type": "socks5",
                "host": str(parsed_profile.hostname or "").strip(),
                "port": parsed_profile.port,
                "username": profile.get("proxy_username"),
                "password": profile.get("proxy_password"),
            }
            proxy_server = self._format_proxy_record_as_url(record)
            if not proxy_server:
                continue
            identity = (
                proxy_server,
                str(profile.get("proxy_username") or "").strip(),
                str(profile.get("proxy_password") or "").strip(),
            )
            if identity in seen:
                continue
            seen.add(identity)
            pool.append(
                {
                    "proxy_server": proxy_server,
                    "proxy_username": str(profile.get("proxy_username") or "").strip() or None,
                    "proxy_password": str(profile.get("proxy_password") or "").strip() or None,
                }
            )

        for data in self.vps_scraper_records_by_key.values():
            route = data.get("route") or {}
            parsed = self._parse_proxy_from_string(str(route.get("proxy_server") or "").strip(), default_type="socks5")
            if not parsed:
                continue
            if str(parsed.get("proxy_type") or "").strip().lower() != "socks5":
                continue
            parsed["username"] = str(route.get("proxy_username") or parsed.get("username") or "").strip() or None
            parsed["password"] = str(route.get("proxy_password") or parsed.get("password") or "").strip() or None
            proxy_server = self._format_proxy_record_as_url(parsed)
            if not proxy_server:
                continue
            identity = (
                proxy_server,
                str(parsed.get("username") or "").strip(),
                str(parsed.get("password") or "").strip(),
            )
            if identity in seen:
                continue
            seen.add(identity)
            pool.append(
                {
                    "proxy_server": proxy_server,
                    "proxy_username": str(parsed.get("username") or "").strip() or None,
                    "proxy_password": str(parsed.get("password") or "").strip() or None,
                }
            )

        return pool

    def _import_proxy_pool_from_file_prompt(self):
        file_path = filedialog.askopenfilename(
            title="Import Auto-Rotation Proxy Pool (TXT/CSV)",
            filetypes=[
                ("Proxy files", "*.txt *.csv"),
                ("Text files", "*.txt"),
                ("CSV files", "*.csv"),
                ("All files", "*.*"),
            ],
            parent=self.settings_window or self.root,
        )
        if not file_path:
            return

        records = self._parse_proxy_records_from_file(file_path, force_socks5=True)
        if not records:
            messagebox.showwarning(
                "No SOCKS5 Proxies",
                "No valid SOCKS5 proxy records were parsed from the selected file.",
            )
            return

        pool_urls = []
        for record in records:
            proxy_url = self._format_proxy_record_as_url(record)
            if proxy_url:
                pool_urls.append(proxy_url)

        if not pool_urls:
            messagebox.showwarning("No SOCKS5 Proxies", "The selected file did not contain usable SOCKS5 proxies.")
            return

        pool_text = ", ".join(pool_urls)
        self.vps_scraper_form_vars["proxy_mode"].set("auto_rotation")
        self.vps_scraper_form_vars["proxy_pool"].set(pool_text)
        if not self.vps_scraper_form_vars["proxy_server"].get().strip():
            self.vps_scraper_form_vars["proxy_server"].set(pool_urls[0])
        self.status_bar.config(text=f"Loaded {len(pool_urls)} SOCKS5 proxies into auto-rotation pool")

    def _extract_proxy_records_from_payload(self, payload, default_type: str = "http", default_country: str = ""):
        records = []
        candidates = payload
        if isinstance(payload, dict):
            for key in ("proxies", "data", "results", "items", "list"):
                if key in payload and isinstance(payload[key], list):
                    candidates = payload[key]
                    break
            else:
                candidates = [payload]

        if not isinstance(candidates, list):
            return records

        for item in candidates:
            if isinstance(item, str):
                parsed = self._parse_proxy_from_string(item, default_type=default_type, default_country=default_country)
                if parsed:
                    records.append(parsed)
                continue

            if not isinstance(item, dict):
                continue

            endpoint = item.get("proxy") or item.get("endpoint") or item.get("url")
            if isinstance(endpoint, str):
                parsed = self._parse_proxy_from_string(endpoint, default_type=default_type, default_country=default_country)
                if parsed:
                    if item.get("country") and not parsed.get("country"):
                        parsed["country"] = str(item.get("country"))
                    records.append(parsed)
                    continue

            host = item.get("host") or item.get("ip") or item.get("server")
            port = item.get("port")
            if not host or not port:
                continue

            try:
                port_value = int(str(port).strip())
            except ValueError:
                continue

            username = item.get("username") or item.get("user") or item.get("login")
            password = item.get("password") or item.get("pass")
            proxy_type = item.get("type") or item.get("protocol") or item.get("scheme") or default_type or "http"
            country = item.get("country") or item.get("region") or default_country or None
            name = item.get("name") or item.get("label") or f"{host}:{port_value}"
            status = item.get("status") or "active"
            notes = item.get("notes") or "Imported from API"

            records.append(
                {
                    "name": str(name),
                    "proxy_type": str(proxy_type),
                    "host": str(host).strip(),
                    "port": port_value,
                    "username": str(username).strip() if username else None,
                    "password": str(password).strip() if password else None,
                    "country": str(country).strip() if country else None,
                    "status": str(status).strip(),
                    "notes": str(notes).strip(),
                }
            )

        return records

    def _upsert_proxy_records(self, records):
        if not records:
            return (0, 0)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        created = 0
        updated = 0
        now_iso = datetime.now().isoformat()

        for record in records:
            host = record["host"]
            port = int(record["port"])
            username = record.get("username")
            cursor.execute(
                """
                SELECT id
                FROM proxies
                WHERE host = ? AND port = ? AND COALESCE(username, '') = COALESCE(?, '')
                """,
                (host, port, username),
            )
            existing = cursor.fetchone()
            if existing:
                cursor.execute(
                    """
                    UPDATE proxies
                    SET name=?, proxy_type=?, password=?, country=?, status=?, notes=?, updated_at=?
                    WHERE id=?
                    """,
                    (
                        record.get("name"),
                        record.get("proxy_type", "http"),
                        record.get("password"),
                        record.get("country"),
                        record.get("status", "active"),
                        record.get("notes"),
                        now_iso,
                        existing[0],
                    ),
                )
                updated += 1
            else:
                cursor.execute(
                    """
                    INSERT INTO proxies (name, proxy_type, host, port, username, password, country, status, notes, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        record.get("name"),
                        record.get("proxy_type", "http"),
                        host,
                        port,
                        username,
                        record.get("password"),
                        record.get("country"),
                        record.get("status", "active"),
                        record.get("notes"),
                        now_iso,
                        now_iso,
                    ),
                )
                created += 1

        conn.commit()
        conn.close()
        return (created, updated)

    def _fetch_proxies_from_api(self):
        if requests is None:
            messagebox.showerror("Dependency Missing", "The 'requests' package is required to fetch proxies from API.")
            return

        api_url = self.scraper_setting_vars["proxy_api_url"].get().strip()
        if not api_url:
            messagebox.showwarning("Missing API URL", "Set Proxy API URL in Scraper settings first.")
            return

        timeout_raw = self.scraper_setting_vars["proxy_api_timeout_seconds"].get().strip() or "20"
        try:
            timeout = max(5, int(timeout_raw))
        except ValueError:
            messagebox.showwarning("Invalid Timeout", "Proxy API timeout must be a number (seconds).")
            return

        api_key = self.scraper_setting_vars["proxy_api_key"].get().strip()
        auth_header = self.scraper_setting_vars["proxy_api_auth_header"].get().strip() or "Authorization"
        default_type = self.scraper_setting_vars["proxy_api_default_type"].get().strip() or "http"
        default_country = self.scraper_setting_vars["proxy_api_default_country"].get().strip()

        headers = {"Accept": "application/json"}
        if api_key:
            if auth_header.lower() == "authorization":
                headers[auth_header] = f"Bearer {api_key}"
            else:
                headers[auth_header] = api_key

        self.status_bar.config(text="Fetching proxies from API...")
        self.root.update()

        try:
            response = requests.get(api_url, headers=headers, timeout=timeout)
            response.raise_for_status()
        except Exception as e:
            messagebox.showerror("Proxy API Error", f"Failed to fetch proxies:\n{e}")
            self.status_bar.config(text="Proxy API fetch failed")
            return

        payload = None
        records = []
        try:
            payload = response.json()
            records = self._extract_proxy_records_from_payload(
                payload,
                default_type=default_type,
                default_country=default_country,
            )
        except ValueError:
            text = response.text.strip()
            if text:
                for line in text.splitlines():
                    parsed = self._parse_proxy_from_string(
                        line,
                        default_type=default_type,
                        default_country=default_country,
                    )
                    if parsed:
                        records.append(parsed)

        if not records:
            messagebox.showwarning(
                "No Proxies Parsed",
                "API call succeeded but no proxy records were recognized.\n"
                "Expected JSON list/items with host/port or proxy endpoint strings.",
            )
            self.status_bar.config(text="Proxy API returned no usable proxies")
            return

        created, updated = self._upsert_proxy_records(records)
        self._refresh_proxy_tree()
        self._refresh_proxy_dropdown()
        self.status_bar.config(text=f"Proxy sync complete: {created} created, {updated} updated")
        messagebox.showinfo("Proxy Sync Complete", f"Imported {len(records)} proxies.\nCreated: {created}\nUpdated: {updated}")

    def _import_proxies_from_text_prompt(self):
        """Import proxies from pasted cURL/proxy lines."""
        pasted = simpledialog.askstring(
            "Paste Proxy or cURL",
            "Paste one or more lines.\nSupported:\n"
            "- curl -v -x socks5://user:pass@host:port ...\n"
            "- socks5://user:pass@host:port\n"
            "- host:port or host:port:user:pass",
            parent=self.settings_window or self.root,
        )
        if pasted is None:
            return

        default_type = self.scraper_setting_vars.get("proxy_api_default_type", tk.StringVar(value="http")).get().strip() or "http"
        default_country = self.scraper_setting_vars.get("proxy_api_default_country", tk.StringVar(value="")).get().strip()

        records = []
        for line in pasted.splitlines():
            parsed = self._parse_proxy_from_string(
                line.strip(),
                default_type=default_type,
                default_country=default_country,
            )
            if parsed:
                records.append(parsed)

        if not records:
            messagebox.showwarning(
                "Nothing Imported",
                "No valid proxy lines were detected in the pasted content.",
            )
            return

        created, updated = self._upsert_proxy_records(records)
        self._refresh_proxy_tree()
        self._refresh_proxy_dropdown()
        self.status_bar.config(text=f"Pasted proxy import complete: {created} created, {updated} updated")
        messagebox.showinfo(
            "Import Complete",
            f"Processed {len(records)} proxy record(s).\nCreated: {created}\nUpdated: {updated}",
        )

    def _delete_selected_proxy(self):
        if not self.selected_proxy_id:
            messagebox.showwarning("No Selection", "Select a proxy first.")
            return
        if not messagebox.askyesno("Delete Proxy", "Delete selected proxy? Any account mapping will be cleared."):
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("UPDATE fb_accounts SET proxy_id = NULL, updated_at = ? WHERE proxy_id = ?", (datetime.now().isoformat(), self.selected_proxy_id))
        cursor.execute("DELETE FROM proxies WHERE id = ?", (self.selected_proxy_id,))
        conn.commit()
        conn.close()

        self._refresh_proxy_tree()
        self._refresh_proxy_dropdown()
        self._refresh_account_tree()
        self._clear_proxy_form()
        self.status_bar.config(text="Proxy deleted")

    def _save_account(self):
        account_name = self.account_form_vars["account_name"].get().strip()
        email = self.account_form_vars["email"].get().strip()
        if not account_name and not email:
            messagebox.showwarning("Validation Error", "Provide account name or email.")
            return

        proxy_label = self.account_form_vars["proxy_label"].get()
        proxy_id = self.proxy_options_by_id.get(proxy_label)
        now_iso = datetime.now().isoformat()

        payload = (
            account_name or None,
            email or None,
            self.account_form_vars["status"].get().strip() or "ACTIVE",
            self.account_form_vars["profile_path"].get().strip() or None,
            self.account_form_vars["user_agent"].get().strip() or None,
            proxy_id,
            self.account_form_vars["notes"].get().strip() or None,
            now_iso,
        )

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        if self.selected_account_id:
            cursor.execute(
                """
                UPDATE fb_accounts
                SET account_name=?, email=?, status=?, profile_path=?, user_agent=?, proxy_id=?, notes=?, updated_at=?
                WHERE id=?
                """,
                payload + (self.selected_account_id,),
            )
        else:
            cursor.execute(
                """
                INSERT INTO fb_accounts (account_name, email, status, profile_path, user_agent, proxy_id, notes, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload + (now_iso,),
            )
        conn.commit()
        conn.close()

        self._refresh_account_tree()
        self._clear_account_form()
        self.status_bar.config(text="Facebook account settings saved")

    def _delete_selected_account(self):
        if not self.selected_account_id:
            messagebox.showwarning("No Selection", "Select an account first.")
            return
        if not messagebox.askyesno("Delete Account", "Delete selected Facebook account config?"):
            return

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM fb_accounts WHERE id = ?", (self.selected_account_id,))
        conn.commit()
        conn.close()

        self._refresh_account_tree()
        self._clear_account_form()
        self.status_bar.config(text="Facebook account deleted")

    def _set_selected_account_status(self, status: str):
        if not self.selected_account_id:
            messagebox.showwarning("No Selection", "Select an account first.")
            return
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE fb_accounts SET status = ?, updated_at = ? WHERE id = ?",
            (status, datetime.now().isoformat(), self.selected_account_id),
        )
        conn.commit()
        conn.close()
        self._refresh_account_tree()
        self.account_form_vars["status"].set(status)
        self.status_bar.config(text=f"Account {self.selected_account_id} status set to {status}")

    def _get_selected_account_proxy_context(self):
        if not self.selected_account_id:
            return None

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, a.account_name, a.email, a.profile_path,
                   p.id, p.proxy_type, p.host, p.port, p.username, p.password
            FROM fb_accounts a
            LEFT JOIN proxies p ON p.id = a.proxy_id
            WHERE a.id = ?
            """,
            (self.selected_account_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None

        return {
            "account_id": row[0],
            "account_name": row[1] or "",
            "email": row[2] or "",
            "profile_path": row[3] or "",
            "proxy_id": row[4],
            "proxy_type": (row[5] or "").lower(),
            "host": row[6],
            "port": row[7],
            "username": row[8],
            "password": row[9],
        }

    def _get_account_context_by_id(self, account_id: int):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, a.account_name, a.email, a.profile_path,
                   p.id, p.proxy_type, p.host, p.port, p.username, p.password
            FROM fb_accounts a
            LEFT JOIN proxies p ON p.id = a.proxy_id
            WHERE a.id = ?
            """,
            (account_id,),
        )
        row = cursor.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "account_id": row[0],
            "account_name": row[1] or "",
            "email": row[2] or "",
            "profile_path": row[3] or "",
            "proxy_id": row[4],
            "proxy_type": (row[5] or "").lower(),
            "host": row[6],
            "port": row[7],
            "username": row[8],
            "password": row[9],
        }

    def _get_active_scraper_account_context(self):
        raw = self._get_scraper_setting("active_scraper_account_id", "")
        if not raw:
            return None
        try:
            account_id = int(raw)
        except ValueError:
            return None
        return self._get_account_context_by_id(account_id)

    def _list_eligible_scraper_account_contexts(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            SELECT a.id, a.account_name, a.email, a.profile_path,
                   UPPER(COALESCE(a.status, 'ACTIVE')),
                   COALESCE(a.cooldown_until, ''),
                   COALESCE(a.last_scrape_started_at, ''),
                   COALESCE(a.updated_at, ''),
                   p.id, p.proxy_type, p.host, p.port, p.username, p.password
            FROM fb_accounts a
            INNER JOIN proxies p ON p.id = a.proxy_id
            WHERE UPPER(COALESCE(a.status, '')) IN ('ACTIVE', 'COOLDOWN')
              AND LOWER(COALESCE(p.proxy_type, '')) = 'socks5'
              AND LOWER(COALESCE(p.status, 'active')) = 'active'
              AND COALESCE(TRIM(p.host), '') != ''
              AND p.port IS NOT NULL
            ORDER BY a.id
            """
        )
        rows = cursor.fetchall()
        conn.close()

        accounts = []
        now = datetime.now()
        for row in rows:
            account_status = (row[4] or "ACTIVE").upper()
            cooldown_until_raw = row[5] or ""
            if account_status == "COOLDOWN":
                try:
                    cooldown_until = datetime.fromisoformat(str(cooldown_until_raw))
                except ValueError:
                    cooldown_until = None
                if cooldown_until and cooldown_until > now:
                    continue
            try:
                port = int(row[11])
            except (TypeError, ValueError):
                continue
            if port <= 0:
                continue
            accounts.append(
                {
                    "account_id": row[0],
                    "account_name": row[1] or "",
                    "email": row[2] or "",
                    "profile_path": row[3] or "",
                    "status": account_status,
                    "cooldown_until": cooldown_until_raw,
                    "last_scrape_started_at": row[6] or "",
                    "updated_at": row[7] or "",
                    "proxy_id": row[8],
                    "proxy_type": (row[9] or "").lower(),
                    "host": row[10],
                    "port": port,
                    "username": row[12],
                    "password": row[13],
                }
            )
        return accounts

    def _select_next_scraper_account_context(self):
        eligible_accounts = self._list_eligible_scraper_account_contexts()
        if not eligible_accounts:
            return None

        try:
            min_reuse_seconds = max(0, int(self._get_scraper_setting("account_min_reuse_seconds", "30").strip()))
        except ValueError:
            min_reuse_seconds = 30

        reusable_accounts = eligible_accounts
        if min_reuse_seconds > 0:
            cutoff = datetime.now().timestamp() - min_reuse_seconds
            candidate_accounts = []
            for account in eligible_accounts:
                last_started_raw = str(account.get("last_scrape_started_at") or "").strip()
                if not last_started_raw:
                    candidate_accounts.append(account)
                    continue
                try:
                    last_started_ts = datetime.fromisoformat(last_started_raw).timestamp()
                except ValueError:
                    candidate_accounts.append(account)
                    continue
                if last_started_ts <= cutoff:
                    candidate_accounts.append(account)
            if candidate_accounts:
                reusable_accounts = candidate_accounts

        ids = [acc["account_id"] for acc in reusable_accounts]

        preferred_ctx = self._get_active_scraper_account_context()
        preferred_id = preferred_ctx["account_id"] if preferred_ctx and preferred_ctx["account_id"] in ids else ids[0]

        last_raw = self._get_scraper_setting("last_scraper_account_id", "").strip()
        try:
            last_id = int(last_raw) if last_raw else None
        except ValueError:
            last_id = None

        if last_id in ids:
            next_index = (ids.index(last_id) + 1) % len(ids)
            return reusable_accounts[next_index]

        for acc in reusable_accounts:
            if acc["account_id"] == preferred_id:
                return acc

        return reusable_accounts[0]

    def _reserve_next_scraper_account_for_run(self):
        account_ctx = self._select_next_scraper_account_context()
        if not account_ctx:
            return None, (
                "No eligible scraper account found. Add at least one ACTIVE Facebook account "
                "with an assigned ACTIVE SOCKS5 proxy."
            )
        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute(
            """
            UPDATE fb_accounts
            SET last_scrape_started_at = ?, updated_at = ?
            WHERE id = ?
            """,
            (now_iso, now_iso, account_ctx["account_id"]),
        )
        conn.commit()
        conn.close()
        self._set_scraper_setting("last_scraper_account_id", str(account_ctx["account_id"]))
        self._refresh_active_scraper_account_label()
        return account_ctx, None

    def _find_free_local_port(self) -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.bind(("127.0.0.1", 0))
            return int(sock.getsockname()[1])

    def _start_local_socks5_auth_bridge(self, account_ctx: dict):
        if importlib.util.find_spec("pproxy") is None:
            raise RuntimeError(
                "SOCKS5 auth bridge requires package 'pproxy'. Install with: pip install pproxy"
            )

        local_port = self._find_free_local_port()
        local_proxy = f"socks5://127.0.0.1:{local_port}"

        user = quote(str(account_ctx.get("username") or ""), safe="")
        password = quote(str(account_ctx.get("password") or ""), safe="")
        auth_fragment = f"#{user}:{password}" if user or password else ""
        upstream = f"socks5://{account_ctx['host']}:{account_ctx['port']}{auth_fragment}"

        cmd = [
            sys.executable,
            "-c",
            "import asyncio,pproxy.server as s;asyncio.set_event_loop(asyncio.new_event_loop());s.main()",
            "-l",
            local_proxy,
            "-r",
            upstream,
            "-ul",
            local_proxy,
            "-ur",
            upstream,
        ]

        process = subprocess.Popen(
            cmd,
            cwd=Path(__file__).parent,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
        )

        # Give bridge time to bind.
        start = datetime.now()
        while (datetime.now() - start).total_seconds() < 8:
            if process.poll() is not None:
                stderr_output = ""
                try:
                    if process.stderr:
                        stderr_output = process.stderr.read().decode("utf-8", errors="ignore")[:500]
                except Exception:
                    pass
                if stderr_output:
                    raise RuntimeError(f"Failed to start local SOCKS5 bridge process: {stderr_output}")
                raise RuntimeError("Failed to start local SOCKS5 bridge process.")
            try:
                with socket.create_connection(("127.0.0.1", local_port), timeout=0.5):
                    return process, local_proxy
            except OSError:
                pass
            time.sleep(0.2)

        try:
            process.terminate()
        except Exception:
            pass
        raise RuntimeError("Timed out waiting for local SOCKS5 bridge to become ready.")

    def _build_playwright_proxy_for_account(self, account_ctx: dict):
        """
        Build Playwright proxy config for account context.
        Returns (proxy_config_or_none, bridge_process_or_none).
        """
        if not account_ctx or not account_ctx.get("proxy_id"):
            return None, None

        proxy_type = (account_ctx.get("proxy_type") or "").lower()
        host = account_ctx.get("host")
        port = account_ctx.get("port")
        if not proxy_type or not host or not port:
            return None, None

        if account_ctx.get("username") or account_ctx.get("password"):
            bridge_process, bridge_proxy = self._start_local_socks5_auth_bridge(account_ctx)
            return {"server": bridge_proxy}, bridge_process

        return {"server": f"{proxy_type}://{host}:{port}"}, None

    async def _run_manual_proxy_login_flow(self, account_ctx: dict):
        from playwright.async_api import async_playwright

        account_id = account_ctx["account_id"]
        profile_path = (account_ctx.get("profile_path") or "").strip()
        if not profile_path:
            profile_path = str(Path(__file__).parent / "browser_profiles" / f"fb_account_{account_id}")
        Path(profile_path).mkdir(parents=True, exist_ok=True)

        proxy_config, bridge_process = self._build_playwright_proxy_for_account(account_ctx)

        result = {
            "profile_path": profile_path,
            "proxy_ip": None,
            "logged_in": False,
            "cookie_count": 0,
            "user_agent": "",
        }

        try:
            async with async_playwright() as p:
                context = await p.chromium.launch_persistent_context(
                    user_data_dir=profile_path,
                    headless=False,
                    proxy=proxy_config,
                    args=[
                        "--disable-quic",
                        "--disable-dns-prefetch",
                        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                        "--disable-features=WebRtcHideLocalIpsWithMdns,DnsOverHttps",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-background-networking",
                        "--disable-sync",
                        "--window-size=1280,900",
                    ],
                    viewport={"width": 1280, "height": 900},
                )

                page = context.pages[0] if context.pages else await context.new_page()

                try:
                    await page.goto("https://ipv4.icanhazip.com", wait_until="domcontentloaded", timeout=60000)
                    result["proxy_ip"] = (await page.text_content("body") or "").strip()
                except Exception:
                    result["proxy_ip"] = "unknown"

                await page.goto("https://www.facebook.com/login", wait_until="domcontentloaded", timeout=60000)
                result["user_agent"] = await page.evaluate("navigator.userAgent")

                deadline = asyncio.get_event_loop().time() + (15 * 60)
                cookies_data = []
                while asyncio.get_event_loop().time() < deadline:
                    cookies_data = await context.cookies("https://www.facebook.com")
                    if any(c.get("name") == "c_user" and c.get("value") for c in cookies_data):
                        result["logged_in"] = True
                        break
                    await asyncio.sleep(2)

                result["cookie_count"] = len(cookies_data)
                result["cookies_json"] = json.dumps(cookies_data)
                await context.close()
        finally:
            if bridge_process and bridge_process.poll() is None:
                try:
                    bridge_process.terminate()
                    bridge_process.wait(timeout=3)
                except Exception:
                    try:
                        bridge_process.kill()
                    except Exception:
                        pass

        return result

    async def _run_account_browse_session(self, account_ctx: dict, start_url: str):
        from playwright.async_api import async_playwright

        account_id = account_ctx["account_id"]
        profile_path = (account_ctx.get("profile_path") or "").strip()
        if not profile_path:
            profile_path = str(Path(__file__).parent / "browser_profiles" / f"fb_account_{account_id}")
        Path(profile_path).mkdir(parents=True, exist_ok=True)

        proxy_config, bridge_process = self._build_playwright_proxy_for_account(account_ctx)
        try:
            async with async_playwright() as p:
                launch_kwargs = {
                    "user_data_dir": profile_path,
                    "headless": False,
                    "args": [
                        "--disable-quic",
                        "--disable-dns-prefetch",
                        "--force-webrtc-ip-handling-policy=disable_non_proxied_udp",
                        "--disable-features=WebRtcHideLocalIpsWithMdns,DnsOverHttps",
                        "--no-first-run",
                        "--no-default-browser-check",
                        "--disable-background-networking",
                        "--disable-sync",
                        "--window-size=1280,900",
                    ],
                    "viewport": {"width": 1280, "height": 900},
                }
                if proxy_config:
                    launch_kwargs["proxy"] = proxy_config

                context = await p.chromium.launch_persistent_context(**launch_kwargs)
                page = context.pages[0] if context.pages else await context.new_page()
                await page.goto(start_url, wait_until="domcontentloaded", timeout=60000)

                while True:
                    open_pages = [pg for pg in context.pages if not pg.is_closed()]
                    if not open_pages:
                        break
                    await asyncio.sleep(1)

                cookies_data = await context.cookies("https://www.facebook.com")
                user_agent = await page.evaluate("navigator.userAgent") if page and not page.is_closed() else ""
                await context.close()
                return {
                    "profile_path": profile_path,
                    "cookies_json": json.dumps(cookies_data),
                    "cookie_count": len(cookies_data),
                    "user_agent": user_agent,
                }
        finally:
            if bridge_process and bridge_process.poll() is None:
                try:
                    bridge_process.terminate()
                    bridge_process.wait(timeout=3)
                except Exception:
                    try:
                        bridge_process.kill()
                    except Exception:
                        pass

    def _start_manual_login_for_selected_account(self):
        account_ctx = self._get_selected_account_proxy_context()
        if not account_ctx:
            messagebox.showwarning("No Selection", "Select an account first.")
            return

        if not account_ctx.get("proxy_id"):
            messagebox.showwarning("Missing Proxy", "Assign a proxy to this account before login.")
            return

        if account_ctx.get("proxy_type") != "socks5":
            messagebox.showwarning(
                "Protocol Required",
                "This login flow is locked to SOCKS5 as requested. Please set proxy type to socks5.",
            )
            return

        account_label = account_ctx.get("account_name") or account_ctx.get("email") or f"Account {account_ctx['account_id']}"
        messagebox.showinfo(
            "Manual Proxy Login",
            "A Chromium window will open using only the assigned SOCKS5 proxy.\n\n"
            "Steps:\n"
            "1. Log in to Facebook in that window\n"
            "2. Stay on Facebook/Marketplace until login completes\n"
            "3. Wait for completion message (up to 15 minutes timeout)\n\n"
            "If proxy auth fails, no login will be saved.",
        )

        result = {"error": None, "data": None}

        def worker():
            try:
                result["data"] = asyncio.run(self._run_manual_proxy_login_flow(account_ctx))
            except Exception as e:
                result["error"] = str(e)

        thread = threading.Thread(target=worker, daemon=True, name="manual-login-worker")
        thread.start()
        self.status_bar.config(text=f"Opening proxied login browser for {account_label}...")

        def on_complete():
            if thread.is_alive():
                self.root.after(500, on_complete)
                return

            if result["error"]:
                self.status_bar.config(text=f"Manual login failed: {result['error']}")
                messagebox.showerror("Login Failed", f"Manual login failed:\n{result['error']}")
                return

            data = result["data"] or {}
            if data.get("logged_in"):
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute(
                    """
                    UPDATE fb_accounts
                    SET status = ?, profile_path = ?, user_agent = ?, cookies_json = ?, last_login_at = ?, updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        "ACTIVE",
                        data.get("profile_path"),
                        data.get("user_agent"),
                        data.get("cookies_json"),
                        datetime.now().isoformat(),
                        datetime.now().isoformat(),
                        account_ctx["account_id"],
                    ),
                )
                conn.commit()
                conn.close()

                self._refresh_account_tree()
                self.account_form_vars["profile_path"].set(data.get("profile_path", ""))
                self.account_form_vars["status"].set("ACTIVE")
                self.account_form_vars["user_agent"].set(data.get("user_agent", ""))
                proxy_ip = data.get("proxy_ip") or "unknown"
                self.status_bar.config(text=f"Manual login complete for {account_label} (proxy IP: {proxy_ip})")
                messagebox.showinfo(
                    "Login Complete",
                    f"Account login saved.\nProxy IP seen by browser: {proxy_ip}\nCookies captured: {data.get('cookie_count', 0)}",
                )
            else:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute(
                    "UPDATE fb_accounts SET status = ?, updated_at = ? WHERE id = ?",
                    ("NEEDS_LOGIN", datetime.now().isoformat(), account_ctx["account_id"]),
                )
                conn.commit()
                conn.close()
                self._refresh_account_tree()
                self.account_form_vars["status"].set("NEEDS_LOGIN")
                self.status_bar.config(text=f"Manual login timed out for {account_label}")
                messagebox.showwarning(
                    "Login Timeout",
                    "Did not detect Facebook login cookie (`c_user`) within 15 minutes.\n"
                    "Account remains NEEDS_LOGIN.",
                )

        self.root.after(500, on_complete)

    def _set_selected_account_as_scraper_account(self):
        account_ctx = self._get_selected_account_proxy_context()
        if not account_ctx:
            messagebox.showwarning("No Selection", "Select an account first.")
            return
        eligible_ids = {acc["account_id"] for acc in self._list_eligible_scraper_account_contexts()}
        if account_ctx["account_id"] not in eligible_ids:
            messagebox.showwarning(
                "Not Eligible",
                "This account is not eligible for scraper runs yet.\n\n"
                "Requirements:\n"
                "- Account status must be ACTIVE\n"
                "- Assigned proxy must be ACTIVE\n"
                "- Proxy type must be SOCKS5\n"
                "- Proxy host/port must be valid",
            )
            return

        self._set_scraper_setting("active_scraper_account_id", str(account_ctx["account_id"]))
        self._set_scraper_setting("last_scraper_account_id", "")
        self._refresh_active_scraper_account_label()
        label = account_ctx.get("account_name") or account_ctx.get("email") or f"Account {account_ctx['account_id']}"
        self.status_bar.config(text=f"Set rotation start account: {label}")
        messagebox.showinfo(
            "Scraper Rotation Start Set",
            f"Rotation will begin from this account on the next run:\n{label}",
        )

    def _open_browser_for_selected_account(self):
        account_ctx = self._get_selected_account_proxy_context()
        if not account_ctx:
            messagebox.showwarning("No Selection", "Select an account first.")
            return
        if not account_ctx.get("proxy_id"):
            messagebox.showwarning("Missing Proxy", "Assign a proxy to this account before browsing.")
            return
        if account_ctx.get("proxy_type") != "socks5":
            messagebox.showwarning(
                "Protocol Required",
                "This browse flow is locked to SOCKS5 as requested. Please set proxy type to socks5.",
            )
            return

        start_url = simpledialog.askstring(
            "Start URL",
            "Open browser at this URL:",
            initialvalue="https://www.facebook.com/marketplace/",
            parent=self.settings_window or self.root,
        )
        if not start_url:
            return

        result = {"error": None, "data": None}
        label = account_ctx.get("account_name") or account_ctx.get("email") or f"Account {account_ctx['account_id']}"

        def worker():
            try:
                result["data"] = asyncio.run(self._run_account_browse_session(account_ctx, start_url))
            except Exception as e:
                result["error"] = str(e)

        thread = threading.Thread(target=worker, daemon=True, name="account-browse-worker")
        thread.start()
        self.status_bar.config(text=f"Opening browser session for {label}...")

        def on_complete():
            if thread.is_alive():
                self.root.after(500, on_complete)
                return

            if result["error"]:
                self.status_bar.config(text=f"Browser session failed: {result['error']}")
                messagebox.showerror("Session Failed", f"Could not open proxied browser:\n{result['error']}")
                return

            data = result.get("data") or {}
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()
            cursor.execute(
                """
                UPDATE fb_accounts
                SET profile_path = ?, user_agent = COALESCE(NULLIF(?, ''), user_agent),
                    cookies_json = ?, updated_at = ?
                WHERE id = ?
                """,
                (
                    data.get("profile_path"),
                    data.get("user_agent", ""),
                    data.get("cookies_json", "[]"),
                    datetime.now().isoformat(),
                    account_ctx["account_id"],
                ),
            )
            conn.commit()
            conn.close()
            self._refresh_account_tree()
            self.status_bar.config(text=f"Closed browser session for {label}")
            messagebox.showinfo(
                "Session Closed",
                f"Browser session closed for {label}.\nCookies captured: {data.get('cookie_count', 0)}",
            )

        self.root.after(500, on_complete)

    def _load_scraper_settings_form(self):
        defaults = {
            "monitor_interval_minutes": "30",
            "monitor_jitter_seconds": "45",
            "account_min_reuse_seconds": "30",
            "scrape_delay_min_seconds": "5",
            "scrape_delay_max_seconds": "12",
            "scrape_scroll_target_cards": "180",
            "scrape_scroll_max_rounds": "14",
            "notify_profitable_only": "1",
            "max_queries_per_run": str(len(SEARCH_QUERIES)),
            "accessory_filter_keywords": self._default_accessory_keyword_csv(),
            "accessory_filter_max_price": "120",
            "dolphin_api_key": "",
            "proxy_api_url": "",
            "proxy_api_key": "",
            "proxy_api_auth_header": "Authorization",
            "proxy_api_timeout_seconds": "20",
            "proxy_api_default_type": "http",
            "proxy_api_default_country": "",
            "server_sync_enabled": "0",
            "server_api_base_url": "https://api.iphoneguy.com.au",
            "server_api_token": "",
            "server_sync_poll_seconds": "2",
            "server_sync_since_id": "0",
            "server_ssh_enabled": "1",
            "server_ssh_user": "ubuntu",
            "server_ssh_host": "15.235.185.32",
            "server_ssh_project_dir": "/home/ubuntu/iphone-flipper-server/server",
            "server_monitor_worker_services": "worker worker_2 worker_3",
        }
        for key, default in defaults.items():
            self.scraper_setting_vars[key].set(self._get_scraper_setting(key, default))

    def _save_scraper_settings(self):
        try:
            monitor_interval = int(self.scraper_setting_vars["monitor_interval_minutes"].get().strip())
            monitor_jitter_seconds = int(self.scraper_setting_vars["monitor_jitter_seconds"].get().strip())
            account_min_reuse_seconds = int(self.scraper_setting_vars["account_min_reuse_seconds"].get().strip())
            delay_min = int(self.scraper_setting_vars["scrape_delay_min_seconds"].get().strip())
            delay_max = int(self.scraper_setting_vars["scrape_delay_max_seconds"].get().strip())
            scroll_target_cards = int(self.scraper_setting_vars["scrape_scroll_target_cards"].get().strip())
            scroll_max_rounds = int(self.scraper_setting_vars["scrape_scroll_max_rounds"].get().strip())
            max_queries = int(self.scraper_setting_vars["max_queries_per_run"].get().strip())
            notify_only = self.scraper_setting_vars["notify_profitable_only"].get().strip()
            server_sync_poll_seconds = int(
                self.scraper_setting_vars["server_sync_poll_seconds"].get().strip() or "2"
            )
        except ValueError:
            messagebox.showwarning("Validation Error", "Numeric settings must be valid integers.")
            return

        if (
            monitor_interval <= 0
            or monitor_jitter_seconds < 0
            or account_min_reuse_seconds < 0
            or delay_min < 0
            or delay_max < delay_min
            or scroll_target_cards < 40
            or scroll_max_rounds < 2
            or max_queries <= 0
            or server_sync_poll_seconds <= 0
        ):
            messagebox.showwarning(
                "Validation Error",
                "Please check setting ranges (positive interval/query/poll, jitter >= 0, account reuse >= 0, max delay >= min delay, scroll target >= 40, scroll rounds >= 2).",
            )
            return

        proxy_timeout_raw = self.scraper_setting_vars["proxy_api_timeout_seconds"].get().strip() or "20"
        try:
            proxy_timeout = max(5, int(proxy_timeout_raw))
        except ValueError:
            messagebox.showwarning("Validation Error", "Proxy API timeout must be an integer >= 5.")
            return

        accessory_keywords_raw = self.scraper_setting_vars["accessory_filter_keywords"].get().strip()
        accessory_keywords = self._parse_accessory_keyword_csv(accessory_keywords_raw)
        if not accessory_keywords:
            accessory_keywords = self._parse_accessory_keyword_csv(self._default_accessory_keyword_csv())
        accessory_keywords_csv = ", ".join(accessory_keywords)

        accessory_max_price = self._safe_float(
            self.scraper_setting_vars["accessory_filter_max_price"].get().strip() or "120"
        )
        if accessory_max_price is None or accessory_max_price <= 0:
            messagebox.showwarning("Validation Error", "Accessory max price must be a number greater than 0.")
            return
        accessory_max_price_text = f"{float(accessory_max_price):.2f}".rstrip("0").rstrip(".")

        updates = {
            "monitor_interval_minutes": str(monitor_interval),
            "monitor_jitter_seconds": str(monitor_jitter_seconds),
            "account_min_reuse_seconds": str(account_min_reuse_seconds),
            "scrape_delay_min_seconds": str(delay_min),
            "scrape_delay_max_seconds": str(delay_max),
            "scrape_scroll_target_cards": str(scroll_target_cards),
            "scrape_scroll_max_rounds": str(scroll_max_rounds),
            "notify_profitable_only": "1" if notify_only in {"1", "true", "True", "yes"} else "0",
            "max_queries_per_run": str(max_queries),
            "accessory_filter_keywords": accessory_keywords_csv,
            "accessory_filter_max_price": accessory_max_price_text,
            "dolphin_api_key": self.scraper_setting_vars["dolphin_api_key"].get().strip(),
            "proxy_api_url": self.scraper_setting_vars["proxy_api_url"].get().strip(),
            "proxy_api_key": self.scraper_setting_vars["proxy_api_key"].get().strip(),
            "proxy_api_auth_header": self.scraper_setting_vars["proxy_api_auth_header"].get().strip() or "Authorization",
            "proxy_api_timeout_seconds": str(proxy_timeout),
            "proxy_api_default_type": self.scraper_setting_vars["proxy_api_default_type"].get().strip() or "http",
            "proxy_api_default_country": self.scraper_setting_vars["proxy_api_default_country"].get().strip(),
            "server_sync_enabled": "1" if self._is_truthy(self.scraper_setting_vars["server_sync_enabled"].get()) else "0",
            "server_api_base_url": self._normalize_server_base_url(self.scraper_setting_vars["server_api_base_url"].get()),
            "server_api_token": self.scraper_setting_vars["server_api_token"].get().strip(),
            "server_sync_poll_seconds": str(server_sync_poll_seconds),
            "server_sync_since_id": str(max(0, self._safe_int(self.scraper_setting_vars["server_sync_since_id"].get(), 0))),
            "server_ssh_enabled": "1" if self._is_truthy(self.scraper_setting_vars["server_ssh_enabled"].get()) else "0",
            "server_ssh_user": self.scraper_setting_vars["server_ssh_user"].get().strip() or "ubuntu",
            "server_ssh_host": self.scraper_setting_vars["server_ssh_host"].get().strip(),
            "server_ssh_project_dir": self.scraper_setting_vars["server_ssh_project_dir"].get().strip() or "/home/ubuntu/iphone-flipper-server/server",
            "server_monitor_worker_services": self.scraper_setting_vars["server_monitor_worker_services"].get().strip() or "worker worker_2 worker_3",
        }

        now_iso = datetime.now().isoformat()
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        for key, value in updates.items():
            cursor.execute(
                """
                INSERT INTO scraper_settings (setting_key, setting_value, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET setting_value=excluded.setting_value, updated_at=excluded.updated_at
                """,
                (key, value, now_iso),
            )
        conn.commit()
        conn.close()
        self._start_or_restart_server_sync_from_settings()
        self.status_bar.config(text="Scraper settings saved")
        messagebox.showinfo("Saved", "Scraper settings updated.")

    def show_settings_manager(self):
        """Open settings manager with Proxies + VPS Scrapers tabs."""
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.lift()
            self.settings_window.focus_force()
            return

        self.selected_proxy_id = None
        self.selected_vps_scraper_key = None
        self.vps_scraper_records_by_key = {}
        self.vps_worker_summary_records_by_key = {}
        self.vps_proxy_profile_options = {}
        self.proxy_form_vars = {
            "name": tk.StringVar(),
            "proxy_type": tk.StringVar(value="http"),
            "host": tk.StringVar(),
            "port": tk.StringVar(),
            "username": tk.StringVar(),
            "password": tk.StringVar(),
            "country": tk.StringVar(),
            "status": tk.StringVar(value="active"),
            "notes": tk.StringVar(),
        }
        legacy_profile_rotation_seconds = self._get_scraper_setting("server_profile_rotation_period_seconds", "8")
        self.vps_scraper_form_vars = {
            "server_sync_enabled": tk.StringVar(value=self._get_scraper_setting("server_sync_enabled", "1")),
            "server_api_base_url": tk.StringVar(value=self._get_scraper_setting("server_api_base_url", "https://api.iphoneguy.com.au")),
            "server_api_token": tk.StringVar(value=self._get_scraper_setting("server_api_token", "")),
            "server_sync_poll_seconds": tk.StringVar(value=self._get_scraper_setting("server_sync_poll_seconds", "2")),
            "server_proxy_rotation_period_seconds": tk.StringVar(
                value=self._get_scraper_setting("server_proxy_rotation_period_seconds", "120")
            ),
            "server_proxy_lease_seconds": tk.StringVar(
                value=self._get_scraper_setting("server_proxy_lease_seconds", "600")
            ),
            "server_worker_1_scrape_frequency_seconds": tk.StringVar(
                value=self._get_scraper_setting("server_worker_1_scrape_frequency_seconds", legacy_profile_rotation_seconds)
            ),
            "server_worker_2_scrape_frequency_seconds": tk.StringVar(
                value=self._get_scraper_setting("server_worker_2_scrape_frequency_seconds", legacy_profile_rotation_seconds)
            ),
            "server_worker_3_scrape_frequency_seconds": tk.StringVar(
                value=self._get_scraper_setting(
                    "server_worker_3_scrape_frequency_seconds",
                    self._get_scraper_setting("server_profile_rotation_period_seconds", "5"),
                )
            ),
            "server_ssh_enabled": tk.StringVar(value=self._get_scraper_setting("server_ssh_enabled", "1")),
            "server_ssh_user": tk.StringVar(value=self._get_scraper_setting("server_ssh_user", "ubuntu")),
            "server_ssh_host": tk.StringVar(value=self._get_scraper_setting("server_ssh_host", "")),
            "server_ssh_project_dir": tk.StringVar(value=self._get_scraper_setting("server_ssh_project_dir", "/home/ubuntu/iphone-flipper-server/server")),
            "server_monitor_worker_services": tk.StringVar(
                value=self._normalize_server_monitor_services(
                    self._get_scraper_setting("server_monitor_worker_services", "worker worker_2 worker_3")
                )
            ),
            "accessory_filter_keywords": tk.StringVar(
                value=self._get_scraper_setting(
                    "accessory_filter_keywords",
                    self._default_accessory_keyword_csv(),
                )
            ),
            "accessory_filter_max_price": tk.StringVar(
                value=self._get_scraper_setting("accessory_filter_max_price", "120")
            ),
            "worker_name": tk.StringVar(),
            "route_name": tk.StringVar(),
            "is_enabled": tk.StringVar(value="1"),
            "priority": tk.StringVar(value="100"),
            "lane_override": tk.StringVar(value="auto"),
            "route_interval_seconds": tk.StringVar(),
            "user_data_dir": tk.StringVar(),
            "proxy_profile": tk.StringVar(value=VPS_PROXY_PROFILE_DIRECT),
            "proxy_mode": tk.StringVar(value="fixed"),
            "proxy_server": tk.StringVar(),
            "proxy_username": tk.StringVar(),
            "proxy_password": tk.StringVar(),
            "proxy_pool": tk.StringVar(),
            "search_queries": tk.StringVar(),
            "manual_login_required": tk.StringVar(value="0"),
            "manual_login_reason": tk.StringVar(),
            "quarantined_at": tk.StringVar(),
            "quarantine_reason": tk.StringVar(),
            "quarantine_evidence": tk.StringVar(),
            "worker_status": tk.StringVar(),
            "worker_scraped_last_minute": tk.StringVar(),
            "worker_lease": tk.StringVar(),
            "worker_cooldown": tk.StringVar(),
            "worker_last_run": tk.StringVar(),
            "computed_lane": tk.StringVar(),
            "effective_lane": tk.StringVar(),
            "priority_score": tk.StringVar(),
            "priority_score_updated_at": tk.StringVar(),
            "profitable_hit_rate": tk.StringVar(),
            "recent_duplicate_ratio": tk.StringVar(),
        }

        window = tk.Toplevel(self.root)
        window.title("Connections & Scraper Settings")
        window.geometry("1360x920")
        window.minsize(1100, 760)
        self.settings_window = window

        def on_close():
            self.settings_window = None
            self.vps_scraper_tree = None
            self.vps_worker_summary_tree = None
            self.proxy_tree = None
            window.destroy()

        window.protocol("WM_DELETE_WINDOW", on_close)

        notebook = ttk.Notebook(window)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        vps_tab = ttk.Frame(notebook)
        notebook.add(vps_tab, text="VPS Scrapers")
        vps_tab.columnconfigure(0, weight=1)
        vps_tab.rowconfigure(0, weight=1)

        vps_scroll_host = ttk.Frame(vps_tab)
        vps_scroll_host.grid(row=0, column=0, sticky="nsew")
        vps_scroll_host.columnconfigure(0, weight=1)
        vps_scroll_host.rowconfigure(0, weight=1)

        vps_canvas = tk.Canvas(vps_scroll_host, highlightthickness=0, bd=0)
        vps_canvas.grid(row=0, column=0, sticky="nsew")
        vps_scrollbar = ttk.Scrollbar(vps_scroll_host, orient="vertical", command=vps_canvas.yview)
        vps_scrollbar.grid(row=0, column=1, sticky="ns")
        vps_canvas.configure(yscrollcommand=vps_scrollbar.set)

        vps_inner = ttk.Frame(vps_canvas)
        vps_inner.columnconfigure(0, weight=1)
        vps_inner_window = vps_canvas.create_window((0, 0), window=vps_inner, anchor="nw")

        def _refresh_vps_scroll_region(event=None):
            vps_canvas.configure(scrollregion=vps_canvas.bbox("all"))

        def _stretch_vps_inner(event):
            vps_canvas.itemconfigure(vps_inner_window, width=event.width)

        vps_inner.bind("<Configure>", _refresh_vps_scroll_region)
        vps_canvas.bind("<Configure>", _stretch_vps_inner)

        def _on_vps_mousewheel(event):
            delta = getattr(event, "delta", 0)
            if delta:
                vps_canvas.yview_scroll(int(-delta / 120), "units")
            elif getattr(event, "num", None) == 4:
                vps_canvas.yview_scroll(-1, "units")
            elif getattr(event, "num", None) == 5:
                vps_canvas.yview_scroll(1, "units")

        vps_canvas.bind("<MouseWheel>", _on_vps_mousewheel)
        vps_canvas.bind("<Button-4>", _on_vps_mousewheel)
        vps_canvas.bind("<Button-5>", _on_vps_mousewheel)
        vps_inner.bind("<MouseWheel>", _on_vps_mousewheel)
        vps_inner.bind("<Button-4>", _on_vps_mousewheel)
        vps_inner.bind("<Button-5>", _on_vps_mousewheel)

        conn_frame = ttk.LabelFrame(vps_inner, text="VPS Connection")
        conn_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 6))
        conn_frame.columnconfigure(1, weight=1)
        conn_frame.columnconfigure(3, weight=1)

        connection_fields = [
            ("Server API Base URL", "server_api_base_url"),
            ("Server API Token", "server_api_token"),
            ("Proxy Reuse Cooldown (seconds)", "server_proxy_rotation_period_seconds"),
            ("Proxy Lease TTL (seconds)", "server_proxy_lease_seconds"),
            ("Worker 1 Scrape Frequency (seconds)", "server_worker_1_scrape_frequency_seconds"),
            ("Worker 2 Scrape Frequency (seconds)", "server_worker_2_scrape_frequency_seconds"),
            ("Worker 3 Scrape Frequency (seconds)", "server_worker_3_scrape_frequency_seconds"),
            ("Server SSH Host", "server_ssh_host"),
            ("Server SSH User", "server_ssh_user"),
            ("Server SSH Project Dir", "server_ssh_project_dir"),
        ]
        for idx, (label, key) in enumerate(connection_fields):
            row = idx // 2
            col_offset = (idx % 2) * 2
            ttk.Label(conn_frame, text=label + ":").grid(row=row, column=col_offset, sticky="w", padx=8, pady=6)
            if key == "server_api_token":
                widget = ttk.Entry(conn_frame, textvariable=self.vps_scraper_form_vars[key], show="*")
            else:
                widget = ttk.Entry(conn_frame, textvariable=self.vps_scraper_form_vars[key])
            widget.grid(row=row, column=col_offset + 1, sticky="ew", padx=8, pady=6)

        conn_actions = ttk.Frame(conn_frame)
        conn_actions_row = ((len(connection_fields) - 1) // 2) + 1
        conn_actions.grid(row=conn_actions_row, column=0, columnspan=4, sticky="ew", padx=8, pady=(8, 10))
        conn_actions.columnconfigure(0, weight=1)
        conn_actions.columnconfigure(1, weight=1)
        conn_actions.columnconfigure(2, weight=1)
        conn_actions.columnconfigure(3, weight=1)
        conn_actions.columnconfigure(4, weight=1)
        ttk.Button(conn_actions, text="Save VPS Connection", command=self._save_vps_connection_settings).grid(
            row=0, column=0, sticky="ew", padx=4
        )
        ttk.Button(conn_actions, text="Open VPS Activity Monitor", command=self.show_server_activity_monitor).grid(
            row=0, column=1, sticky="ew", padx=4
        )
        ttk.Button(conn_actions, text="Refresh Routes", command=self._refresh_vps_scraper_tree).grid(
            row=0, column=2, sticky="ew", padx=4
        )
        ttk.Button(conn_actions, text="Start Scrapers", command=self.start_server_scrapers).grid(
            row=0, column=3, sticky="ew", padx=4
        )
        ttk.Button(conn_actions, text="Stop Scrapers", command=self.stop_server_scrapers).grid(
            row=0, column=4, sticky="ew", padx=4
        )
        ttk.Button(conn_actions, text="Add/Edit Route", command=self._show_vps_route_editor).grid(
            row=0, column=5, sticky="ew", padx=4
        )

        # -----------------------------
        # Dolphin Profiles Tab
        # -----------------------------
        dolphin_tab = ttk.Frame(notebook)
        notebook.add(dolphin_tab, text="Dolphin Profiles")
        dolphin_tab.columnconfigure(0, weight=1)
        dolphin_tab.rowconfigure(0, weight=1)

        dolphin_table_frame = ttk.Frame(dolphin_tab)
        dolphin_table_frame.grid(row=0, column=0, sticky="nsew", padx=10, pady=10)
        dolphin_table_frame.rowconfigure(0, weight=1)
        dolphin_table_frame.columnconfigure(0, weight=1)

        self.dolphin_tree = ttk.Treeview(
            dolphin_table_frame,
            columns=("ID", "Name", "Intervention Req", "Status", "Browser", "Tags", "Memory"),
            show="headings",
            selectmode="extended",
        )
        for col, width in [
            ("ID", 80),
            ("Name", 180),
            ("Intervention Req", 120),
            ("Status", 100),
            ("Browser", 120),
            ("Tags", 150),
            ("Memory", 80),
        ]:
            self.dolphin_tree.heading(col, text=col)
            self.dolphin_tree.column(col, width=width, anchor=tk.W)
            
        dolphin_vsb = ttk.Scrollbar(dolphin_table_frame, orient="vertical", command=self.dolphin_tree.yview)
        dolphin_hsb = ttk.Scrollbar(dolphin_table_frame, orient="horizontal", command=self.dolphin_tree.xview)
        self.dolphin_tree.configure(yscrollcommand=dolphin_vsb.set, xscrollcommand=dolphin_hsb.set)
        self.dolphin_tree.grid(row=0, column=0, sticky="nsew")
        dolphin_vsb.grid(row=0, column=1, sticky="ns")
        dolphin_hsb.grid(row=1, column=0, sticky="ew")

        # Dolphin Anty API Configuration
        dolphin_key_frame = ttk.LabelFrame(dolphin_tab, text="Dolphin Anty API Configuration")
        dolphin_key_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 6))
        dolphin_key_frame.columnconfigure(1, weight=1)
        dolphin_key_frame.columnconfigure(3, weight=1)
        
        ttk.Label(dolphin_key_frame, text="API Key:").grid(row=0, column=0, sticky="w", padx=8, pady=6)
        dolphin_key_entry = ttk.Entry(dolphin_key_frame, textvariable=self.scraper_setting_vars.setdefault("dolphin_api_key", tk.StringVar(value=self._get_scraper_setting("dolphin_api_key", ""))), show="*")
        dolphin_key_entry.grid(row=0, column=1, sticky="ew", padx=8, pady=6)

        ttk.Label(dolphin_key_frame, text="API URL:").grid(row=0, column=2, sticky="w", padx=8, pady=6)
        dolphin_url_entry = ttk.Entry(dolphin_key_frame, textvariable=self.scraper_setting_vars.setdefault("dolphin_api_url", tk.StringVar(value=self._get_scraper_setting("dolphin_api_url", "http://localhost:3001"))))
        dolphin_url_entry.grid(row=0, column=3, sticky="ew", padx=8, pady=6)

        ttk.Button(
            dolphin_key_frame,
            text="Save Settings",
            command=lambda: [
                self._set_scraper_setting("dolphin_api_key", self.scraper_setting_vars["dolphin_api_key"].get().strip()),
                self._set_scraper_setting("dolphin_api_url", self.scraper_setting_vars["dolphin_api_url"].get().strip() or "http://localhost:3001"),
                self.status_bar.config(text="Dolphin API settings saved.")
            ]
        ).grid(row=0, column=4, sticky="e", padx=8, pady=6)

        dolphin_buttons = ttk.Frame(dolphin_tab)
        dolphin_buttons.grid(row=2, column=0, sticky="ew", padx=10, pady=(0, 10))
        
        ttk.Button(dolphin_buttons, text="Fetch Profiles", command=self._refresh_dolphin_profiles).pack(side=tk.LEFT, padx=5)
        ttk.Button(dolphin_buttons, text="Start Selected", command=self._start_selected_dolphin_profiles).pack(side=tk.LEFT, padx=5)
        ttk.Button(dolphin_buttons, text="Stop Selected", command=self._stop_selected_dolphin_profiles).pack(side=tk.LEFT, padx=5)

        accessory_frame = ttk.LabelFrame(vps_inner, text="Accessory Filter (Auto Exclusion)")
        accessory_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=(0, 6))
        accessory_frame.columnconfigure(1, weight=1)
        accessory_frame.columnconfigure(3, weight=1)

        ttk.Label(accessory_frame, text="Accessory Keywords (CSV):").grid(
            row=0, column=0, sticky="w", padx=8, pady=6
        )
        ttk.Entry(
            accessory_frame,
            textvariable=self.vps_scraper_form_vars["accessory_filter_keywords"],
        ).grid(row=0, column=1, columnspan=3, sticky="ew", padx=8, pady=6)

        ttk.Label(accessory_frame, text="Accessory Max Price:").grid(
            row=1, column=0, sticky="w", padx=8, pady=6
        )
        ttk.Entry(
            accessory_frame,
            textvariable=self.vps_scraper_form_vars["accessory_filter_max_price"],
            width=18,
        ).grid(row=1, column=1, sticky="w", padx=8, pady=6)
        ttk.Label(
            accessory_frame,
            text="Use Save VPS Connection to apply",
        ).grid(row=1, column=2, columnspan=2, sticky="w", padx=8, pady=6)

        content = ttk.Frame(vps_inner)
        content.grid(row=2, column=0, sticky="nsew", padx=10, pady=(0, 10))
        content.columnconfigure(0, weight=1)
        content.rowconfigure(0, weight=1)
        content.rowconfigure(1, weight=3)

        summary_frame = ttk.LabelFrame(content, text="Worker Summary")
        summary_frame.grid(row=0, column=0, sticky="nsew", pady=(0, 8))
        summary_frame.rowconfigure(0, weight=1)
        summary_frame.columnconfigure(0, weight=1)

        self.vps_worker_summary_tree = ttk.Treeview(
            summary_frame,
            columns=(
                "Worker",
                "Active Route",
                "Active Lane",
                "Routes",
                "Lane Mix",
                "Status",
                "Scraped (1m)",
                "Last run",
            ),
            show="headings",
            selectmode="browse",
            height=4,
        )
        for col, width in [
            ("Worker", 120),
            ("Active Route", 220),
            ("Active Lane", 110),
            ("Routes", 90),
            ("Lane Mix", 220),
            ("Status", 160),
            ("Scraped (1m)", 120),
            ("Last run", 170),
        ]:
            self.vps_worker_summary_tree.heading(col, text=col)
            self.vps_worker_summary_tree.column(col, width=width, anchor=tk.W)
        summary_vsb = ttk.Scrollbar(summary_frame, orient="vertical", command=self.vps_worker_summary_tree.yview)
        summary_hsb = ttk.Scrollbar(summary_frame, orient="horizontal", command=self.vps_worker_summary_tree.xview)
        self.vps_worker_summary_tree.configure(yscrollcommand=summary_vsb.set, xscrollcommand=summary_hsb.set)
        self.vps_worker_summary_tree.grid(row=0, column=0, sticky="nsew")
        summary_vsb.grid(row=0, column=1, sticky="ns")
        summary_hsb.grid(row=1, column=0, sticky="ew")
        self.vps_worker_summary_tree.bind("<<TreeviewSelect>>", self._on_vps_worker_summary_selected)

        table_frame = ttk.Frame(content)
        table_frame.grid(row=1, column=0, sticky="nsew")
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        self.vps_scraper_tree = ttk.Treeview(
            table_frame,
            columns=(
                "Legacy Worker",
                "Route",
                "Queries",
                "Lane",
                "Score",
                "Status",
                "Scraped (1m)",
                "Last run",
            ),
            show="headings",
            selectmode="browse",
        )
        for col, width in [
            ("Legacy Worker", 160),
            ("Route", 220),
            ("Queries", 180),
            ("Lane", 100),
            ("Score", 90),
            ("Status", 180),
            ("Scraped (1m)", 140),
            ("Last run", 180),
        ]:
            self.vps_scraper_tree.heading(col, text=col)
            self.vps_scraper_tree.column(col, width=width, anchor=tk.W)
        vps_vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.vps_scraper_tree.yview)
        vps_hsb = ttk.Scrollbar(table_frame, orient="horizontal", command=self.vps_scraper_tree.xview)
        self.vps_scraper_tree.configure(yscrollcommand=vps_vsb.set, xscrollcommand=vps_hsb.set)
        self.vps_scraper_tree.grid(row=0, column=0, sticky="nsew")
        vps_vsb.grid(row=0, column=1, sticky="ns")
        vps_hsb.grid(row=1, column=0, sticky="ew")
        self.vps_scraper_tree.bind("<<TreeviewSelect>>", self._on_vps_scraper_selected)

        self._refresh_vps_scraper_tree(preserve_selection=False)
        self._refresh_dolphin_profiles()
        self._clear_vps_scraper_form()

    def _show_vps_route_editor(self, event=None):
        """Open a modal popup to add or edit a VPS Scraper route."""
        editor = tk.Toplevel(self.settings_window)
        editor.title("VPS Route Settings")
        editor.geometry("470x760")
        editor.grab_set()

        form_frame = ttk.Frame(editor, padding=10)
        form_frame.pack(fill=tk.BOTH, expand=True)

        fields = [
            ("Legacy Worker Origin (read-only)", "worker_name"),
            ("Execution Profiles (managed separately)", "user_data_dir"),
            ("Route Name (Identifier)", "route_name"),
            ("Scrape Search Queries (CSV)", "search_queries"),
        ]

        # Use defaults if nothing is selected
        if not self.selected_vps_scraper_key:
            self._clear_vps_scraper_form()

        # Generate inputs
        for idx, (label, key) in enumerate(fields):
            ttk.Label(form_frame, text=label + ":").pack(anchor="w", pady=(8, 2))
            entry = ttk.Entry(form_frame, textvariable=self.vps_scraper_form_vars[key])
            if key in {"worker_name", "user_data_dir"}:
                entry.state(["readonly"])
            entry.pack(fill="x")

        ttk.Label(form_frame, text="Lane Override:").pack(anchor="w", pady=(8, 2))
        lane_override_dropdown = ttk.Combobox(
            form_frame,
            textvariable=self.vps_scraper_form_vars["lane_override"],
            state="readonly",
        )
        lane_override_dropdown["values"] = ("auto", "hot", "warm", "sweep")
        lane_override_dropdown.pack(fill="x")

        ttk.Label(form_frame, text="Proxy Profile:").pack(anchor="w", pady=(8, 2))
        proxy_dropdown = ttk.Combobox(
            form_frame,
            textvariable=self.vps_scraper_form_vars["proxy_profile"],
            state="readonly",
        )
        proxy_dropdown["values"] = list(self.vps_proxy_profile_options.keys()) or [
            VPS_PROXY_PROFILE_DIRECT,
            VPS_PROXY_PROFILE_CUSTOM,
        ]
        proxy_dropdown.pack(fill="x")

        # Custom Proxy Overrides
        custom_frame = ttk.LabelFrame(form_frame, text="Optional Route Proxy Override (IP:PORT)")
        custom_frame.pack(fill="x", pady=(10, 0), ipady=5)
        ttk.Entry(custom_frame, textvariable=self.vps_scraper_form_vars["proxy_server"]).pack(fill="x", padx=5, pady=5)
        
        ttk.Checkbutton(
            form_frame,
            text="Enable Route",
            variable=self.vps_scraper_form_vars["is_enabled"],
            onvalue="1",
            offvalue="0"
        ).pack(anchor="w", pady=(10, 0))

        manual_login_checkbox = ttk.Checkbutton(
            form_frame,
            text="Manual Login is managed on execution profiles",
            variable=self.vps_scraper_form_vars["manual_login_required"],
            onvalue="1",
            offvalue="0"
        )
        manual_login_checkbox.state(["disabled"])
        manual_login_checkbox.pack(anchor="w", pady=(5, 0))

        computed_lane_frame = ttk.LabelFrame(form_frame, text="Computed Scheduler State")
        computed_lane_frame.pack(fill="x", pady=(12, 0), ipady=4)
        for label, key in (
            ("Computed Lane", "computed_lane"),
            ("Effective Lane", "effective_lane"),
            ("Priority Score", "priority_score"),
            ("Profitable Hit Rate", "profitable_hit_rate"),
            ("Duplicate Ratio", "recent_duplicate_ratio"),
            ("Score Updated At", "priority_score_updated_at"),
        ):
            row = ttk.Frame(computed_lane_frame)
            row.pack(fill="x", padx=6, pady=2)
            ttk.Label(row, text=f"{label}:").pack(side=tk.LEFT)
            ttk.Label(row, textvariable=self.vps_scraper_form_vars[key]).pack(side=tk.LEFT, padx=(6, 0))

        # Actions
        btn_frame = ttk.Frame(form_frame)
        btn_frame.pack(fill="x", pady=20)
        
        def save_and_close():
            self._save_vps_scraper_route(allow_remap=True)
            editor.destroy()
            
        def delete_and_close():
            self._delete_vps_scraper_route()
            editor.destroy()

        ttk.Button(btn_frame, text="Save Route", command=save_and_close).pack(side=tk.LEFT, expand=True, padx=2)
        ttk.Button(btn_frame, text="Delete Route", command=delete_and_close).pack(side=tk.LEFT, expand=True, padx=2)
        ttk.Button(btn_frame, text="Cancel", command=editor.destroy).pack(side=tk.LEFT, expand=True, padx=2)

    # ===== Price Sheet Functions =====

    def _price_sheet_file_path(self) -> Path:
        return Path(__file__).parent / "price_list.csv"

    def _price_sheet_headers(self):
        return [
            "Model",
            "Buying Price",
            "Selling Price",
            "Backglass repair cost",
            "Screen repair cost",
            "Battery repair cost",
            "Camera lens repair cost",
        ]

    @staticmethod
    def _normalize_price_sheet_model(model: str) -> str:
        return re.sub(r"\s+", " ", str(model or "").strip()).lower()

    def _load_price_sheet_rows(self):
        rows = []
        path = self._price_sheet_file_path()
        if not path.exists():
            return rows

        with open(path, "r", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                normalized = {header: str(row.get(header, "")).strip() for header in self._price_sheet_headers()}
                if normalized["Model"]:
                    rows.append(normalized)
        return rows

    def _render_price_sheet_tree(self):
        if not self.price_sheet_tree:
            return

        for item in self.price_sheet_tree.get_children():
            self.price_sheet_tree.delete(item)

        for idx, row in enumerate(self.price_sheet_rows):
            self.price_sheet_tree.insert(
                "",
                tk.END,
                iid=str(idx),
                values=(
                    row["Model"],
                    row["Buying Price"],
                    row["Selling Price"],
                    row["Backglass repair cost"],
                    row["Screen repair cost"],
                    row["Battery repair cost"],
                    row["Camera lens repair cost"],
                ),
            )

    def _clear_price_sheet_form(self):
        self.price_sheet_selected_index = None
        for var in self.price_form_vars.values():
            var.set("")
        if self.price_sheet_tree:
            self.price_sheet_tree.selection_remove(self.price_sheet_tree.selection())

    def _on_price_sheet_select(self, event=None):
        if not self.price_sheet_tree:
            return

        selection = self.price_sheet_tree.selection()
        if not selection:
            return

        index = int(selection[0])
        if index < 0 or index >= len(self.price_sheet_rows):
            return

        self.price_sheet_selected_index = index
        row = self.price_sheet_rows[index]
        for header, var in self.price_form_vars.items():
            var.set(row.get(header, ""))

    def _upsert_price_sheet_row(self):
        model = self.price_form_vars["Model"].get().strip()
        if not model:
            messagebox.showwarning("Validation Error", "Model is required.")
            return

        numeric_headers = [h for h in self._price_sheet_headers() if h != "Model"]
        row_data = {"Model": model}

        for header in numeric_headers:
            raw = self.price_form_vars[header].get().strip()
            if not raw:
                messagebox.showwarning("Validation Error", f"{header} is required.")
                return
            try:
                row_data[header] = str(float(raw))
            except ValueError:
                messagebox.showwarning("Validation Error", f"{header} must be a number.")
                return

        normalized_model = self._normalize_price_sheet_model(model)
        had_existing_model = any(
            self._normalize_price_sheet_model(row.get("Model", "")) == normalized_model
            for row in self.price_sheet_rows
        )

        rebuilt_rows = []
        for idx, existing_row in enumerate(self.price_sheet_rows):
            if self.price_sheet_selected_index is not None and idx == self.price_sheet_selected_index:
                continue
            if self._normalize_price_sheet_model(existing_row.get("Model", "")) == normalized_model:
                continue
            rebuilt_rows.append(existing_row)
        rebuilt_rows.append(row_data)

        self.price_sheet_rows = rebuilt_rows

        self.price_sheet_rows.sort(key=lambda r: r["Model"].lower())
        self.price_sheet_selected_index = next(
            (
                idx
                for idx, row in enumerate(self.price_sheet_rows)
                if self._normalize_price_sheet_model(row.get("Model", "")) == normalized_model
            ),
            None,
        )
        self._render_price_sheet_tree()
        action_label = "updated" if had_existing_model else "added"
        self.status_bar.config(text=f"Price row {action_label} for {model}")

    def _delete_price_sheet_row(self):
        if self.price_sheet_selected_index is None:
            messagebox.showwarning("No Selection", "Select a price row to delete.")
            return

        row = self.price_sheet_rows[self.price_sheet_selected_index]
        model = row.get("Model", "selected model")
        if not messagebox.askyesno("Confirm Delete", f"Delete price row for {model}?"):
            return

        del self.price_sheet_rows[self.price_sheet_selected_index]
        self._clear_price_sheet_form()
        self._render_price_sheet_tree()
        self.status_bar.config(text=f"Deleted price row for {model}")

    def _save_price_sheet_to_file(self):
        if not self.price_sheet_rows:
            messagebox.showwarning("No Data", "Price sheet is empty.")
            return

        path = self._price_sheet_file_path()
        headers = self._price_sheet_headers()
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            for row in sorted(self.price_sheet_rows, key=lambda r: r["Model"].lower()):
                writer.writerow({h: row.get(h, "") for h in headers})

        updated = recalculate_listing_financials()
        deal_tracker.update_listing_conversion_scores()
        self.refresh_listings()
        messagebox.showinfo(
            "Price Sheet Saved",
            f"Saved {len(self.price_sheet_rows)} rows.\nRecalculated {updated} listings.",
        )
        self.status_bar.config(text=f"Saved price sheet and recalculated {updated} listings")

    def recalculate_all_listing_values(self):
        """Recalculate listing max buy/profit values from the current price sheet."""
        updated = recalculate_listing_financials()
        deal_tracker.update_listing_conversion_scores()
        self.refresh_listings()
        self.status_bar.config(text=f"Recalculated {updated} listings from price sheet")
        messagebox.showinfo("Recalculation Complete", f"Updated {updated} listings.")

    def show_price_sheet_editor(self):
        """Open the in-app price sheet editor."""
        if self.price_sheet_window and self.price_sheet_window.winfo_exists():
            self.price_sheet_window.lift()
            self.price_sheet_window.focus_force()
            return

        self.price_sheet_rows = self._load_price_sheet_rows()
        self.price_sheet_selected_index = None
        self.price_form_vars = {header: tk.StringVar() for header in self._price_sheet_headers()}

        window = tk.Toplevel(self.root)
        window.title("Price Sheet Editor")
        window.geometry("1180x620")
        self.price_sheet_window = window

        main = ttk.Frame(window)
        main.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        main.columnconfigure(0, weight=3)
        main.columnconfigure(1, weight=2)
        main.rowconfigure(0, weight=1)

        table_frame = ttk.Frame(main)
        table_frame.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        table_frame.rowconfigure(0, weight=1)
        table_frame.columnconfigure(0, weight=1)

        columns = ("Model", "Buy", "Sell", "Backglass", "Screen", "Battery", "Camera")
        self.price_sheet_tree = ttk.Treeview(table_frame, columns=columns, show="headings", selectmode="browse")
        headings = {
            "Model": "Model",
            "Buy": "Max Buy",
            "Sell": "Sell Price",
            "Backglass": "Backglass",
            "Screen": "Screen",
            "Battery": "Battery",
            "Camera": "Camera",
        }
        widths = {"Model": 190, "Buy": 90, "Sell": 90, "Backglass": 90, "Screen": 90, "Battery": 90, "Camera": 90}
        for col in columns:
            self.price_sheet_tree.heading(col, text=headings[col])
            self.price_sheet_tree.column(col, width=widths[col], anchor=tk.W)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.price_sheet_tree.yview)
        self.price_sheet_tree.configure(yscrollcommand=vsb.set)
        self.price_sheet_tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        self.price_sheet_tree.bind("<<TreeviewSelect>>", self._on_price_sheet_select)

        form_frame = ttk.LabelFrame(main, text="Edit Price Row")
        form_frame.grid(row=0, column=1, sticky="nsew")
        form_frame.columnconfigure(1, weight=1)

        row = 0
        for header in self._price_sheet_headers():
            ttk.Label(form_frame, text=header + ":").grid(row=row, column=0, sticky="w", padx=8, pady=6)
            ttk.Entry(form_frame, textvariable=self.price_form_vars[header]).grid(
                row=row, column=1, sticky="ew", padx=8, pady=6
            )
            row += 1

        buttons = ttk.Frame(form_frame)
        buttons.grid(row=row, column=0, columnspan=2, sticky="ew", padx=8, pady=10)
        buttons.columnconfigure(0, weight=1)
        buttons.columnconfigure(1, weight=1)

        ttk.Button(buttons, text="New / Clear", command=self._clear_price_sheet_form).grid(
            row=0, column=0, sticky="ew", padx=4
        )
        ttk.Button(buttons, text="Save Row", command=self._upsert_price_sheet_row).grid(
            row=0, column=1, sticky="ew", padx=4
        )
        ttk.Button(buttons, text="Delete Row", command=self._delete_price_sheet_row).grid(
            row=1, column=0, sticky="ew", padx=4, pady=(8, 0)
        )
        ttk.Button(buttons, text="Apply Changes", command=self._save_price_sheet_to_file).grid(
            row=1, column=1, sticky="ew", padx=4, pady=(8, 0)
        )

        ttk.Button(form_frame, text="Reload From File", command=lambda: self._reload_price_sheet()).grid(
            row=row + 1, column=0, columnspan=2, sticky="ew", padx=8, pady=(4, 8)
        )

        self._render_price_sheet_tree()
        self.status_bar.config(text=f"Loaded {len(self.price_sheet_rows)} price rows")

    def _reload_price_sheet(self):
        self.price_sheet_rows = self._load_price_sheet_rows()
        self._clear_price_sheet_form()
        self._render_price_sheet_tree()
        self.status_bar.config(text=f"Reloaded {len(self.price_sheet_rows)} price rows from file")
        
    def show_about(self):
        """Show about dialog."""
        messagebox.showinfo(
            "About iPhone Flipper Pro",
            "iPhone Flipper Pro v9.0\n\n"
            "Automated iPhone flipping assistant for Perth, Australia.\n\n"
            "Features:\n"
            "• Automated Facebook Marketplace scraping\n"
            "• AI-powered negotiation assistance\n"
            "• Deal tracking and analytics\n"
            "• Pattern learning for better deals\n\n"
            "Powered by Gemini 3 Flash Preview"
        )

    def show_launch_guide(self):
        """Show setup and launch commands in-app."""
        project_dir = str(Path(__file__).parent)
        guide_text = (
            "iPhone Flipper - Setup & Launch Guide\n"
            "=====================================\n\n"
            "1) Open Terminal and go to the project folder:\n"
            f"cd \"{project_dir}\"\n\n"
            "2) Activate virtual environment:\n"
            "source .venv/bin/activate\n\n"
            "3) Launch GUI:\n"
            "python run_gui.py\n\n"
            "First-time setup (if needed)\n"
            "----------------------------\n"
            "python3 -m venv .venv\n"
            "source .venv/bin/activate\n"
            "pip install -r requirements.txt\n"
            "python -m playwright install chromium\n\n"
            "First Facebook login (one-time)\n"
            "-------------------------------\n"
            "python main.py scrape --no-headless\n\n"
            "AI authentication setup\n"
            "-----------------------\n"
            "python main.py login-ai\n"
            "python main.py auth-status\n"
        )

        window = tk.Toplevel(self.root)
        window.title("Setup & Launch Guide")
        window.geometry("760x560")

        text = scrolledtext.ScrolledText(window, wrap=tk.WORD, font=("Menlo", 11))
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.insert(1.0, guide_text)
        text.config(state=tk.DISABLED)

    def _get_dolphin_api_key(self) -> str:
        """Get the dolphin API key, preferring the live GUI value."""
        if hasattr(self, "scraper_setting_vars") and "dolphin_api_key" in self.scraper_setting_vars:
            return self.scraper_setting_vars["dolphin_api_key"].get().strip()
        return self._get_scraper_setting("dolphin_api_key", "").strip()

    def _dolphin_auth_headers(self) -> dict:
        """Build the Authorization header for Dolphin Anty API requests."""
        api_key = self._get_dolphin_api_key()
        if api_key:
            return {"Authorization": f"Bearer {api_key}"}
        return {}

    def _ensure_dolphin_logged_in(self) -> bool:
        """Log in to the Dolphin Anty local API using login-with-token.
        
        The Dolphin Anty desktop app exposes a local API that requires a
        session login via POST /v1.0/auth/login-with-token before it will
        accept other API calls.  This method performs that login and waits
        a moment for the app to establish its session.
        Returns True on success, False on failure.
        """
        api_url = self._get_dolphin_api_url()
        api_key = self._get_dolphin_api_key()
        
        if not api_key:
            return True  # No key configured, let the request fail naturally
        
        # Quick check: does the API already accept requests?
        try:
            probe = requests.get(f"{api_url}/v1.0/browser_profiles", headers=self._dolphin_auth_headers(), timeout=5)
            if probe.status_code == 200:
                return True  # Already logged in
        except Exception:
            pass
        
        # Not logged in — call login-with-token
        self.status_bar.config(text="Logging in to Dolphin Anty…")
        self.root.update_idletasks()
        
        try:
            login_resp = requests.post(
                f"{api_url}/v1.0/auth/login-with-token",
                json={"token": api_key},
                timeout=10,
            )
            login_data = login_resp.json() if login_resp.status_code == 200 else {}
            
            if login_data.get("success"):
                import time
                time.sleep(3)  # Give the app time to establish the session
                self.status_bar.config(text="Dolphin Anty login successful ✓")
                return True
            else:
                err = login_data.get("error", f"HTTP {login_resp.status_code}")
                self.status_bar.config(text=f"Dolphin login failed: {err}")
                return False
        except Exception as e:
            self.status_bar.config(text=f"Dolphin login error: {e}")
            return False
        
    def _get_dolphin_api_url(self) -> str:
        """Get the base Dolphin Anty API URL, defaulting to localhost:3001 if empty."""
        # Prefer live GUI value if available, else fallback to DB
        if hasattr(self, "scraper_setting_vars") and "dolphin_api_url" in self.scraper_setting_vars:
            url = self.scraper_setting_vars["dolphin_api_url"].get().strip()
        else:
            url = self._get_scraper_setting("dolphin_api_url", "http://localhost:3001").strip()
            
        url = url.rstrip("/")
        if not url:
            url = "http://localhost:3001"
            
        if not url.startswith("http"):
            url = "http://" + url
            
        return url

    def _ensure_dolphin_ssh_tunnel(self) -> bool:
        """Ensure an SSH tunnel is open for Dolphin Anty API access.
        
        If the configured Dolphin API URL targets localhost:3001 and port 3001
        is not reachable, automatically create an SSH tunnel to the VPS.
        Returns True if the API should be reachable, False if tunnel setup failed.
        """
        import socket
        
        api_url = self._get_dolphin_api_url()
        
        # Only auto-tunnel when targeting localhost:3001
        if "localhost:3001" not in api_url and "127.0.0.1:3001" not in api_url:
            return True
        
        # Quick check: is port 3001 already reachable?
        try:
            with socket.create_connection(("127.0.0.1", 3001), timeout=1):
                return True
        except (ConnectionRefusedError, OSError, socket.timeout):
            pass
        
        # Port not reachable — try to create SSH tunnel
        ssh_host = self._get_scraper_setting("server_ssh_host", "").strip()
        ssh_user = self._get_scraper_setting("server_ssh_user", "ubuntu").strip()
        
        if not ssh_host:
            self.status_bar.config(text="⚠ No SSH host configured. Set server_ssh_host in Settings.")
            return False
        
        self.status_bar.config(text=f"Opening SSH tunnel to {ssh_host}:3001…")
        self.root.update_idletasks()
        
        try:
            proc = subprocess.Popen(
                [
                    "ssh",
                    "-L", "3001:127.0.0.1:3001",
                    f"{ssh_user}@{ssh_host}",
                    "-N",                       # no remote command
                    "-f",                        # go to background
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "ConnectTimeout=8",
                    "-o", "ExitOnForwardFailure=yes",
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
            _, stderr = proc.communicate(timeout=12)
            
            if proc.returncode != 0:
                err_msg = stderr.decode(errors="replace").strip()
                self.status_bar.config(text=f"SSH tunnel failed: {err_msg}")
                return False
            
            # Give the tunnel a moment to become ready
            import time
            for _ in range(5):
                try:
                    with socket.create_connection(("127.0.0.1", 3001), timeout=1):
                        self.status_bar.config(text="SSH tunnel established ✓")
                        return True
                except (ConnectionRefusedError, OSError, socket.timeout):
                    time.sleep(0.5)
            
            self.status_bar.config(text="SSH tunnel started but port 3001 not yet reachable.")
            return True  # Let the actual API call try anyway
            
        except subprocess.TimeoutExpired:
            self.status_bar.config(text="SSH tunnel timed out.")
            return False
        except FileNotFoundError:
            self.status_bar.config(text="SSH not found. Install OpenSSH.")
            return False
        except Exception as e:
            self.status_bar.config(text=f"SSH tunnel error: {e}")
            return False

    def _refresh_dolphin_profiles(self):
        """Fetch profiles from the Dolphin Anty Cloud API and populate the tree."""
        if not hasattr(self, "dolphin_tree") or not self.dolphin_tree:
            return
        
        # Cloud API is used for listing profiles (no SSH tunnel needed)
        CLOUD_API = "https://anty-api.com"
            
        try:
            headers = self._dolphin_auth_headers()
            if not headers:
                messagebox.showerror("Error", "No Dolphin API Key configured. Enter your API key in Settings.")
                return
            
            resp = requests.get(f"{CLOUD_API}/browser_profiles", headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                profiles = data.get("data", [])
                
                # Clear existing
                for row_id in self.dolphin_tree.get_children():
                    self.dolphin_tree.delete(row_id)
                
                for p in profiles:
                    pid = p.get("id")
                    name = p.get("name", "")
                    tags = ", ".join(p.get("tags", []))
                    browser_type = p.get("browserType", "")
                    memory = p.get("memory", {}).get("value", "")
                    
                    # Extract status from profile data
                    status_info = p.get("status", {})
                    status = status_info.get("name", "Ready") if isinstance(status_info, dict) else "Ready"
                    
                    intervention_req = "Yes" if "MANUAL" in name.upper() or "LOGIN" in name.upper() else "No"
                    
                    self.dolphin_tree.insert(
                        "",
                        tk.END,
                        iid=str(pid),
                        text=str(pid),
                        values=(pid, name, intervention_req, status, browser_type, tags, memory)
                    )
                
                self.status_bar.config(text=f"Fetched {len(profiles)} Dolphin profiles.")
            elif resp.status_code == 401:
                messagebox.showerror("Error", "Invalid API Key. Go to Dolphin Anty → API section to get a valid token.")
            else:
                messagebox.showerror("Error", f"Failed to fetch profiles. Status: {resp.status_code}")
        except Exception as e:
            messagebox.showerror("Error", f"Failed to contact Dolphin Anty Cloud API.\n\nDetails: {e}")

    def _start_selected_dolphin_profiles(self):
        """Start the selected Dolphin Anty profiles on the local machine."""
        if not hasattr(self, "dolphin_tree") or not self.dolphin_tree:
            return
        
        if not self._ensure_dolphin_ssh_tunnel():
            return
            
        selected = self.dolphin_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "No profiles selected.")
            return
            
        started = 0
        errors = []
        for item_id in selected:
            profile_id = self.dolphin_tree.item(item_id, "values")[0]
            try:
                api_url = self._get_dolphin_api_url()
                resp = requests.get(f"{api_url}/v1.0/browser_profiles/{profile_id}/start?automation=1", headers=self._dolphin_auth_headers(), timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        started += 1
                        # Update status in tree if successful
                        current_values = list(self.dolphin_tree.item(item_id, "values"))
                        current_values[2] = f"Running (Port: {data.get('automation', {}).get('port', 'Unknown')})"
                        self.dolphin_tree.item(item_id, values=current_values)
                    else:
                        errors.append(f"Profile {profile_id}: {data.get('msg', 'Unknown error')}")
                else:
                    errors.append(f"Profile {profile_id}: HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"Profile {profile_id}: {e}")
                
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... and {len(errors) - 5} more."
            messagebox.showerror("Dolphin Start Errors", f"Failed to start some profiles:\n{error_msg}")
            
        self.status_bar.config(text=f"Sent start command to {started} profiles.")

    def _stop_selected_dolphin_profiles(self):
        """Stop the selected Dolphin Anty profiles."""
        if not hasattr(self, "dolphin_tree") or not self.dolphin_tree:
            return
        
        if not self._ensure_dolphin_ssh_tunnel():
            return
            
        selected = self.dolphin_tree.selection()
        if not selected:
            messagebox.showinfo("Info", "No profiles selected.")
            return
            
        stopped = 0
        errors = []
        for item_id in selected:
            profile_id = self.dolphin_tree.item(item_id, "values")[0]
            try:
                api_url = self._get_dolphin_api_url()
                resp = requests.get(f"{api_url}/v1.0/browser_profiles/{profile_id}/stop", headers=self._dolphin_auth_headers(), timeout=15)
                if resp.status_code == 200:
                    data = resp.json()
                    if data.get("success"):
                        stopped += 1
                        # Update status in tree
                        current_values = list(self.dolphin_tree.item(item_id, "values"))
                        current_values[2] = "Stopped"
                        self.dolphin_tree.item(item_id, values=current_values)
                    else:
                        errors.append(f"Profile {profile_id}: {data.get('msg', 'Unknown error')}")
                else:
                    errors.append(f"Profile {profile_id}: HTTP {resp.status_code}")
            except Exception as e:
                errors.append(f"Profile {profile_id}: {e}")
                
        if errors:
            error_msg = "\n".join(errors[:5])
            if len(errors) > 5:
                error_msg += f"\n... and {len(errors) - 5} more."
            messagebox.showerror("Dolphin Stop Errors", f"Failed to stop some profiles:\n{error_msg}")
                
        self.status_bar.config(text=f"Sent stop command to {stopped} profiles.")


def main():
    """Main entry point for the GUI."""
    root = tk.Tk()
    app = iPhoneFlipperGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()
