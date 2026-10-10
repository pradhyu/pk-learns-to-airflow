"""
debug_utils.py
==============
Helper utility for attaching Neovim (nvim-dap / debugpy) to running Airflow tasks inside Docker.
"""

from __future__ import annotations

import os
import sys


def attach_neovim_debugger(port: int = 5678, wait: bool = True, force: bool = False):
    """
    Listens on 0.0.0.0:port and pauses task execution until Neovim DAP connects.

    Activation:
    - Activated if `AIRFLOW_DEBUG=true` is set in environment, OR
    - Activated if `force=True` is passed when invoking.

    Usage inside any @task:
        from debug_utils import attach_neovim_debugger
        attach_neovim_debugger()
    """
    is_debug_enabled = os.environ.get("AIRFLOW_DEBUG", "").lower() in ("true", "1", "yes")

    if not is_debug_enabled and not force:
        return

    try:
        import debugpy

        if not debugpy.is_client_connected():
            try:
                debugpy.listen(("0.0.0.0", port))
            except RuntimeError:
                # Already listening on this port in current process
                pass

            print("\n" + "=" * 62)
            print(f"🛑 [Neovim DAP] Debugger listening on 0.0.0.0:{port}")
            print(f"   1. Open DAG in Neovim and toggle breakpoints (<leader>db)")
            print(f"   2. Press <leader>dc and select 'Airflow: Attach to Docker'")
            print("=" * 62)
            sys.stdout.flush()

            if wait:
                print(f"⏳ [Neovim DAP] Execution paused. Waiting for Neovim to attach...")
                sys.stdout.flush()
                debugpy.wait_for_client()
                print("✅ [Neovim DAP] Neovim debugger attached! Continuing execution...\n")
                sys.stdout.flush()
    except ImportError:
        print("⚠️ [Neovim DAP] debugpy is not installed in the Airflow environment.")
    except Exception as exc:
        print(f"⚠️ [Neovim DAP] Could not initialize debugpy: {exc}")
