"""
Copyright (C) 2026 Lightwell

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from hashlib import sha256

from packaging.version import InvalidVersion, Version

from import_orchestrator.engine.errors import TriggerError


def encode_version(version: str) -> str:
    """Normalize a PEP 440 version and encode it for labels and OCI tags.

    Keep the original version in pipeline parameters and source references.
    Use different separators for epochs and local versions to keep their
    artifact tags distinct (for example, ``1!2`` versus ``1+2``).
    """
    try:
        normalized = str(Version(version))
    except InvalidVersion as exc:
        raise TriggerError(f"Invalid Python version: {version!r}") from exc
    return normalized.replace("!", "_").replace("+", "__")


def _shorten(value: str, max_length: int) -> str:
    """Keep a readable prefix and a hash of the full identity when shortening."""
    if len(value) <= max_length:
        return value
    digest = sha256(value.encode()).hexdigest()[:16]
    return f"{value[: max_length - 17]}-{digest}"


def version_label(version: str) -> str:
    """Return a version label within Kubernetes' 63-character limit."""
    return _shorten(encode_version(version), 63)


def image_tag(package: str, version: str) -> str:
    """Return an OCI tag, reserving four characters for the ``.src`` suffix."""
    return _shorten(f"{package}-{encode_version(version)}", 124)
