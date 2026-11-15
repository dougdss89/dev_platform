#!/usr/bin/env python3
"""Gerencia o ciclo de vida (export / create / update) de um Genie Space como código.

Lê o JSON versionado em ``conf/genie/customer_genie_space.json``, substitui o
catálogo base (``advworks_dev``) pelo catálogo do ambiente alvo (ex.: ``advworks``)
e invoca o Databricks CLI (``databricks genie``).

Exemplos de uso:
  1) Criar o Genie Space pela primeira vez no ambiente (retorna o space_id):
     python scripts/deploy_genie_space.py --create \
         --warehouse-id <WAREHOUSE_ID> \
         --catalog advworks_dev \
         --profile community

  2) Atualizar um Genie Space existente (dev ou CI/CD prod):
     # Local (dev):
     python scripts/deploy_genie_space.py \
         --space-id <SPACE_ID_DEV> \
         --catalog advworks_dev \
         --profile community

     # No GitHub Actions (prod, usando variáveis OAuth do Service Principal sem --profile):
     python scripts/deploy_genie_space.py \
         --space-id <SPACE_ID_PROD> \
         --catalog advworks

  3) Exportar alterações feitas na UI de volta para o JSON versionado no Git:
     python scripts/deploy_genie_space.py --export <SPACE_ID_DEV> --profile community
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

DEFAULT_JSON_PATH = Path(__file__).resolve().parents[1] / "conf" / "genie" / "customer_genie_space.json"
SOURCE_CATALOG_PLACEHOLDER = "advworks_dev"


def _run_databricks_cli(args: list[str], profile: str | None = None) -> str:
    cmd = ["databricks", *args]
    if profile:
        cmd.extend(["--profile", profile])
    result = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Erro ao executar {' '.join(cmd)} (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout


def default_parent_path(profile: str | None) -> str:
    """Pasta do usuário autenticado (pessoa ou Service Principal) no workspace alvo."""
    me = json.loads(_run_databricks_cli(["current-user", "me", "-o", "json"], profile=profile))
    return f"/Workspace/Users/{me['userName']}"


def load_and_remap_payload(json_path: Path, target_catalog: str) -> str:
    """Lê o arquivo JSON versionado, troca o catálogo e devolve uma string JSON compacta."""
    raw_text = json_path.read_text(encoding="utf-8")
    remapped_text = raw_text.replace(SOURCE_CATALOG_PLACEHOLDER, target_catalog)
    parsed = json.loads(remapped_text)
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":"))


def export_space(space_id: str, json_path: Path, current_catalog: str, profile: str | None) -> None:
    """Exporta o serialized_space do Databricks para o arquivo JSON local (normalizando o catálogo)."""
    output = _run_databricks_cli(
        ["genie", "get-space", space_id, "--include-serialized-space", "-o", "json"],
        profile=profile,
    )
    space_obj = json.loads(output)
    serialized_raw = space_obj.get("serialized_space")
    if not serialized_raw:
        raise RuntimeError(
            f"O Genie Space '{space_id}' retornou 'serialized_space' vazio. "
            "Verifique se o seu usuário/SP possui permissão CAN EDIT no Genie Space."
        )
    normalized_raw = serialized_raw.replace(current_catalog, SOURCE_CATALOG_PLACEHOLDER)
    parsed_space = json.loads(normalized_raw)
    json_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(json.dumps(parsed_space, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Genie Space '{space_id}' exportado com sucesso para {json_path}")


def create_space(
    warehouse_id: str,
    title: str,
    parent_path: str,
    json_path: Path,
    target_catalog: str,
    profile: str | None,
) -> None:
    """Cria um novo Genie Space no workspace alvo a partir do arquivo JSON."""
    compact_payload = load_and_remap_payload(json_path, target_catalog)
    output = _run_databricks_cli(
        [
            "genie",
            "create-space",
            warehouse_id,
            compact_payload,
            "--title",
            title,
            "--description",
            f"Genie Space de Clientes ({target_catalog}.trusted.customer) gerenciado via DAB/Git",
            "--parent-path",
            parent_path,
            "-o",
            "json",
        ],
        profile=profile,
    )
    created = json.loads(output)
    print(f"Genie Space criado com sucesso! space_id: {created.get('space_id')}")


def update_space(space_id: str, json_path: Path, target_catalog: str, profile: str | None) -> None:
    """Atualiza um Genie Space existente mantendo a mesma URL/ID."""
    compact_payload = load_and_remap_payload(json_path, target_catalog)
    _run_databricks_cli(
        [
            "genie",
            "update-space",
            space_id,
            "--serialized-space",
            compact_payload,
            "-o",
            "json",
        ],
        profile=profile,
    )
    print(f"Genie Space '{space_id}' atualizado com sucesso para o catálogo '{target_catalog}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Deploy e versionamento de Genie Space para DABs.")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--space-id", help="ID de um Genie Space existente para atualizar (update-space)")
    group.add_argument("--create", action="store_true", help="Cria um novo Genie Space (create-space)")
    group.add_argument("--export", metavar="SPACE_ID", help="Exporta um Genie Space existente para o arquivo JSON")

    parser.add_argument("--catalog", default="advworks_dev", help="Catálogo alvo (default: advworks_dev)")
    parser.add_argument("--warehouse-id", help="ID do SQL Warehouse (obrigatório com --create)")
    parser.add_argument("--title", default="AdventureWorks - Customer Analytics", help="Título do Genie Space")
    parser.add_argument(
        "--parent-path",
        default=None,
        help="Pasta no Workspace onde o Genie Space será criado (usado com --create). "
        "Default: /Workspace/Users/<usuário autenticado>",
    )
    parser.add_argument("--json-path", type=Path, default=DEFAULT_JSON_PATH, help="Caminho do JSON do Genie Space")
    parser.add_argument("--profile", default=None, help="Profile do Databricks CLI (omita no CI/CD)")

    args = parser.parse_args()

    if args.export:
        export_space(args.export, args.json_path, args.catalog, args.profile)
    elif args.create:
        if not args.warehouse_id:
            parser.error("--warehouse-id é obrigatório ao usar --create")
        create_space(
            warehouse_id=args.warehouse_id,
            title=args.title,
            parent_path=args.parent_path or default_parent_path(args.profile),
            json_path=args.json_path,
            target_catalog=args.catalog,
            profile=args.profile,
        )
    else:
        update_space(args.space_id, args.json_path, args.catalog, args.profile)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"[ERRO] {exc}", file=sys.stderr)
        sys.exit(1)

