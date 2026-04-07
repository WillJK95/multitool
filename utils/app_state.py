# multitool/utils/app_state.py
"""Application-wide persistent state that survives module navigation."""


class AppState:
    """Shared state that persists across module create/destroy cycles.

    Instantiated once in ``App.__init__`` and accessible from every
    investigation module via ``self.app_state``.  Module instances may be
    destroyed and recreated on each navigation event — this object is
    **not** tied to any single module's lifetime.

    ``safe_go_back()`` must **never** clear entries on this object.
    """

    def __init__(self):
        self.ubo_working_set = None             # list of company dicts
        self.network_working_set = None         # graph-ready payload
        self.network_working_set_source = None  # "Bulk Entity Search" or "UBO Tracer"
        self.recent_edd_reports = []            # list of {name, path, date}
        self.quick_launch_history = []          # lightweight, last 5 entities resolved
        self.network_analytics_snapshot = None   # dict, persists across navigation
