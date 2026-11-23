"""Python package identities must fit Kubernetes and OCI metadata constraints."""

import re

import pytest

from import_orchestrator.engine.errors import TriggerError
from tests.ecosystems.python.test_pipelinerun import _manifest as build_manifest
from tests.ecosystems.python_sdist.test_pipelinerun import _manifest as ingest_manifest


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
@pytest.mark.parametrize(
    ("version", "encoded"),
    [
        ("4!10.0.28000.2957", "4_10.0.28000.2957"),
        ("1.0+vendor.1", "1.0__vendor.1"),
        ("2!1.0+vendor.1", "2_1.0__vendor.1"),
        (" \t\r\n\v\f1.0\u00a0\u2003", "1.0"),
        ("v1.0", "1.0"),
        ("V01.002", "1.2"),
        ("1.0ALPHA", "1.0a0"),
        ("1.0-beta_02", "1.0b2"),
        ("1.0_preview-3", "1.0rc3"),
        ("1.0pre", "1.0rc0"),
        ("1.0c1", "1.0rc1"),
        ("1.0.RC.2", "1.0rc2"),
        ("1.0-1", "1.0.post1"),
        ("1.0rev", "1.0.post0"),
        ("1.0_r4", "1.0.post4"),
        ("1.0-post_2", "1.0.post2"),
        ("1.0DEV", "1.0.dev0"),
        ("1.0-dev_2", "1.0.dev2"),
        ("1.0+Vendor-ABC_001", "1.0__vendor.abc.1"),
        (" v02!01.0-RC_2-post3-dev4+Vendor_001 ", "2_1.0rc2.post3.dev4__vendor.1"),
    ],
)
def test_python_version_is_encoded_only_in_metadata(builder, version, encoded):
    manifest = builder(version=version)
    assert manifest["metadata"]["labels"]["lightwell.redhat.com/version"] == encoded
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["VERSION"] == version
    assert params["IMAGE"].endswith(f":foolib-{encoded}")
    assert params["ociStorage"] == params["IMAGE"] + ".src"
    if "LIGHTWELL_BUILDS_TAG" in params:
        assert params["LIGHTWELL_BUILDS_TAG"] == f"foolib/{version}"


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
def test_long_python_version_produces_valid_label_without_changing_version(builder):
    version = "1.0+" + "vendor." * 10 + "1"
    manifest = builder(version=version)
    label = manifest["metadata"]["labels"]["lightwell.redhat.com/version"]
    assert len(label) <= 63
    assert re.fullmatch(r"[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?", label)
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["VERSION"] == version


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
def test_epoch_and_local_versions_have_distinct_artifact_tags(builder):
    images = []
    for version in ("1!2", "1+2"):
        params = {p["name"]: p["value"] for p in builder(version=version)["spec"]["params"]}
        images.append(params["IMAGE"])
    assert images[0] != images[1]


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
@pytest.mark.parametrize("local_length", [113, 114, 115, 300])
def test_long_versions_fit_both_image_and_source_tags(builder, local_length):
    version = "1+" + "a" * local_length
    manifest = builder(version=version)
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    image_tag = params["IMAGE"].rsplit(":", 1)[1]
    source_tag = params["ociStorage"].rsplit(":", 1)[1]
    assert re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", image_tag)
    assert re.fullmatch(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}", source_tag)
    assert source_tag == image_tag + ".src"
    assert params["VERSION"] == version
    if local_length <= 114:
        assert image_tag == "foolib-1__" + "a" * local_length


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
def test_long_versions_do_not_collide_after_shortening(builder):
    identities = []
    for version in ("1+" + "a" * 300 + "1", "1+" + "a" * 300 + "2"):
        manifest = builder(version=version)
        params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
        identities.append((manifest["metadata"]["labels"]["lightwell.redhat.com/version"], params["IMAGE"]))
    assert identities[0][0] != identities[1][0]
    assert identities[0][1] != identities[1][1]


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
def test_equivalent_long_version_spellings_have_same_metadata(builder):
    version = "1+" + "a" * 300
    plain = builder(version=version)
    padded = builder(version=f" \tV{version}\n")
    assert plain["metadata"]["labels"] == padded["metadata"]["labels"]
    plain_params = {p["name"]: p["value"] for p in plain["spec"]["params"]}
    padded_params = {p["name"]: p["value"] for p in padded["spec"]["params"]}
    assert plain_params["IMAGE"] == padded_params["IMAGE"]


@pytest.mark.parametrize("builder", [build_manifest, ingest_manifest])
@pytest.mark.parametrize("version", [" ", "not-a-version", "1.0/foo", "1.0+", "1..0"])
def test_invalid_python_versions_raise_trigger_error(builder, version):
    with pytest.raises(TriggerError, match="Invalid Python version"):
        builder(version=version)


def test_git_ingestion_encodes_version_without_changing_upstream_ref():
    manifest = ingest_manifest(
        version=" \tV2!1.0+Vendor_1\n",
        source_type="git",
        upstream_url="https://example.test/foolib.git",
        upstream_ref="v2!1.0+Vendor_1",
    )
    assert manifest["metadata"]["labels"]["lightwell.redhat.com/version"] == "2_1.0__vendor.1"
    params = {p["name"]: p["value"] for p in manifest["spec"]["params"]}
    assert params["VERSION"] == " \tV2!1.0+Vendor_1\n"
    assert params["UPSTREAM_REF"] == "v2!1.0+Vendor_1"
    assert params["IMAGE"].endswith(":foolib-2_1.0__vendor.1")
