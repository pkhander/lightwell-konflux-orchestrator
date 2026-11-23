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
from import_orchestrator.ecosystems.javascript import config
from import_orchestrator.ecosystems.javascript.pipelinerun import build_pipelinerun_manifest, parse_ref
from import_orchestrator.engine.pipeline_loader import load_pipeline


class JavaScriptEcosystem:
    """JavaScript npm validated factory builds via javascript-validated-build."""

    name = "javascript"
    default_db_path = config.JAVASCRIPT_DEFAULT_DB_PATH
    pipelinerun_prefix = config.PIPELINERUN_PREFIX
    namespace = config.NAMESPACE

    def build_pipelinerun(self, ref: str, args: argparse.Namespace) -> dict:
        """Build a PipelineRun manifest for one ``package==version`` reference."""
        package, version = parse_ref(ref)
        target = getattr(args, "target", config.DEFAULT_TARGET)
        cfg = config.TARGET_CONFIGS[target]
        pipeline_spec = load_pipeline(config.pipeline_definition_path())
        return build_pipelinerun_manifest(
            package=package,
            version=version,
            pipeline_spec=pipeline_spec,
            namespace=self.namespace,
            application=cfg["app"],
            component=cfg["component"],
            service_account=cfg["service_account"],
            prefix=self.pipelinerun_prefix,
            image_repo_base=config.IMAGE_REPO_BASE,
            source_git_url=getattr(args, "source_git_url", config.DEFAULT_SOURCE_GIT_URL),
            source_revision=getattr(args, "source_revision", config.DEFAULT_SOURCE_REVISION),
            builder_image=getattr(args, "builder_image", config.DEFAULT_BUILDER_IMAGE),
            task_build_npm_package_bundle=getattr(
                args, "task_build_npm_package_bundle", config.DEFAULT_TASK_BUILD_NPM_PACKAGE_BUNDLE
            ),
            task_assess_npm_compliance_bundle=getattr(
                args,
                "task_assess_npm_compliance_bundle",
                config.DEFAULT_TASK_ASSESS_NPM_COMPLIANCE_BUNDLE,
            ),
            tl_npm_registry_url=(
                getattr(args, "tl_npm_registry_url", None)
                or cfg.get("tl_npm_registry_url")
                or config.DEFAULT_TL_NPM_REGISTRY_URL
            ),
            pulp_read_secret_name=getattr(args, "pulp_read_secret_name", config.DEFAULT_PULP_READ_SECRET_NAME),
            git_auth_secret=getattr(args, "git_auth_secret", config.GIT_AUTH_SECRET),
        )

    def request_identity(self, ref: str, args: argparse.Namespace) -> dict[str, str]:
        """Return ADR-0001 Request fingerprint fields for a package==version ref."""
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
            source_repo=getattr(args, "source_git_url", config.DEFAULT_SOURCE_GIT_URL),
        )

    def target_skip_release(self, target: str) -> bool:
        """Return whether the named target should skip Release CR creation."""
        return config.TARGET_CONFIGS.get(target, {}).get("skip_release", False)

    def register_cli(self, subparsers: argparse._SubParsersAction) -> None:
        """Register ``javascript`` CLI commands (build-only; no promote yet)."""
        from import_orchestrator.commands import import_file
        from import_orchestrator.ecosystems.javascript.commands import orchestrate, run, trigger

        eco_parser = subparsers.add_parser(
            "javascript",
            help="JavaScript npm validated factory builds",
        )
        eco_sub = eco_parser.add_subparsers(dest="command")
        import_file.register(eco_sub, self)
        orchestrate.register(eco_sub, self)
        trigger.register(eco_sub, self)
        run.register(eco_sub, self)
        eco_parser.set_defaults(_ecosystem_parser=eco_parser)
