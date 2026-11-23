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

from __future__ import annotations

import argparse

from import_orchestrator.ecosystems.base import build_request_identity
from import_orchestrator.ecosystems.python import config
from import_orchestrator.ecosystems.python.pipelinerun import build_pipelinerun_manifest, parse_ref, resolve_repo_url
from import_orchestrator.engine.pipeline_loader import apply_pipeline_overrides, load_pipeline


class PythonEcosystem:
    """Python ecosystem: Python wheel builds via the python-build pipeline."""

    name = "python"
    default_db_path = config.PYTHON_DEFAULT_DB_PATH
    pipelinerun_prefix = config.PIPELINERUN_PREFIX
    namespace = config.NAMESPACE

    def build_pipelinerun(self, ref: str, args: argparse.Namespace) -> dict:
        package, version = parse_ref(ref)
        target = getattr(args, "target", config.DEFAULT_TARGET)
        builds_tag = getattr(args, "builds_tag", None)
        cfg = config.TARGET_CONFIGS[target]
        # Default build_type from the target config when it pins one (e.g. SCRATCH),
        # else Validated targets ship upstream versions and everything else defaults
        # to a Backport remediated build. An explicit --build-type always wins.
        default_build_type = cfg.get("build_type") or (
            "Validated" if target in ("VALIDATED", "VALIDATED-STAGE") else "Backport"
        )
        build_type = getattr(args, "build_type", None) or default_build_type
        pipeline_path = config.pipeline_definition_path()
        pipeline_spec = load_pipeline(pipeline_path)
        manifest = build_pipelinerun_manifest(
            package=package,
            version=version,
            pipeline_spec=pipeline_spec,
            namespace=self.namespace,
            application=cfg["app"],
            component=cfg["component"],
            service_account=cfg["service_account"],
            prefix=self.pipelinerun_prefix,
            repo_base=getattr(args, "lightwell_builds_repo_base", config.LIGHTWELL_BUILDS_REPO_BASE),
            repo_url=getattr(args, "lightwell_builds_repo_url", None),
            image_repo_base=config.IMAGE_REPO_BASE,
            git_auth_secret=config.GIT_AUTH_SECRET,
            builds_tag=builds_tag,
            build_type=build_type,
            wheel_server_url=getattr(args, "wheel_server_url", None),
            python_builder_image=getattr(args, "python_builder_image", None),
        )
        return apply_pipeline_overrides(manifest, pipeline_path)

    def request_identity(self, ref: str, args: argparse.Namespace) -> dict[str, str]:
        package, _ = parse_ref(ref)
        target = getattr(args, "target", config.DEFAULT_TARGET)
        cfg = config.TARGET_CONFIGS[target]
        return build_request_identity(
            namespace=self.namespace,
            application=cfg["app"],
            component=cfg["component"],
            pipeline=self.pipelinerun_prefix,
            release_plan=getattr(args, "release_plan", "") or "",
            target=target,
            artifact=ref,
            source_repo=resolve_repo_url(
                package,
                getattr(args, "lightwell_builds_repo_base", config.LIGHTWELL_BUILDS_REPO_BASE),
                getattr(args, "lightwell_builds_repo_url", None),
            ),
        )

    def target_skip_release(self, target: str) -> bool:
        return config.TARGET_CONFIGS.get(target, {}).get("skip_release", False)

    def register_cli(self, subparsers: argparse._SubParsersAction) -> None:
        from import_orchestrator.commands import import_file
        from import_orchestrator.ecosystems.python.commands import orchestrate, promote, run, trigger

        eco_parser = subparsers.add_parser("python", help="Python CVE-remediated wheel builds")
        eco_sub = eco_parser.add_subparsers(dest="command")
        import_file.register(eco_sub, self)
        orchestrate.register(eco_sub, self)
        promote.register(eco_sub, self)
        run.register(eco_sub, self)
        trigger.register(eco_sub, self)
        eco_parser.set_defaults(_ecosystem_parser=eco_parser)
