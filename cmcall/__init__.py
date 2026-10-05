"""cmcall - test Delta Chat calls (signalling + WebRTC media) between chatmail relays."""

__all__ = ["__version__"]

try:
    from importlib.metadata import version as _version

    __version__ = _version("cmcall")
except Exception:  # running from a source checkout
    __version__ = "0.0.0+local"
