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
from import_orchestrator.ecosystems.python_sdist import config
from import_orchestrator.ecosystems.python_sdist.canonical_source import resolve_canonical_source
from import_orchestrator.ecosystems.python_sdist.pipelinerun import build_pipelinerun_manifest, parse_ref
from import_orchestrator.engine.pipeline_loader import load_pipeline


class PythonSdistEcosystem:
    """Python sdist ecosystem: Ingests upstream source distributions via python-sdist-ingest."""

    name = "python-sdist"
    default_db_path = config.PYTHON_SDIST_DEFAULT_DB_PATH
    pipelinerun_prefix = config.PIPELINERUN_PREFIX
    namespace = config.NAMESPACE

    def build_pipelinerun(self, ref: str, args: argparse.Namespace) -> dict:
        package, version = parse_ref(ref)
        target = getattr(args, "target", config.DEFAULT_TARGET)
        cfg = config.TARGET_CONFIGS[target]
        pipeline_git_url, pipeline_revision = config.pipeline_source_identity()
        source = resolve_canonical_source(package, version)

        common_kwargs = {
            "package": package,
            "version": version,
            "namespace": self.namespace,
            "application": cfg["app"],
            "component": cfg["component"],
            "service_account": cfg["service_account"],
            "prefix": self.pipelinerun_prefix,
            "image_repo_base": config.IMAGE_REPO_BASE,
            "pipeline_git_url": pipeline_git_url,
            "pipeline_revision": pipeline_revision,
        }

        if source["type"] == "git":
            pipeline_spec = load_pipeline(config.git_pipeline_definition_path())
            return build_pipelinerun_manifest(
                **common_kwargs,
                pipeline_spec=pipeline_spec,
                source_type="git",
                upstream_url=source["upstream_url"],
                upstream_ref=source["ref"],
            )

        source_registries = getattr(args, "source_registries", "rhtl")
        pipeline_spec = load_pipeline(config.pipeline_definition_path())
        return build_pipelinerun_manifest(
            **common_kwargs,
            pipeline_spec=pipeline_spec,
            source_registries=source_registries,
        )

    def request_identity(self, ref: str, args: argparse.Namespace) -> dict[str, str]:
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
        )

    def target_skip_release(self, target: str) -> bool:
        return config.TARGET_CONFIGS.get(target, {}).get("skip_release", False)

    def register_cli(self, subparsers: argparse._SubParsersAction) -> None:
        from import_orchestrator.commands import import_file
        from import_orchestrator.ecosystems.python_sdist.commands import orchestrate, run, trigger

        eco_parser = subparsers.add_parser("python-sdist", help="Python source distribution ingestion (Phase 1)")
        eco_sub = eco_parser.add_subparsers(dest="command")
        import_file.register(eco_sub, self)
        orchestrate.register(eco_sub, self)
        run.register(eco_sub, self)
        trigger.register(eco_sub, self)
        eco_parser.set_defaults(_ecosystem_parser=eco_parser)
