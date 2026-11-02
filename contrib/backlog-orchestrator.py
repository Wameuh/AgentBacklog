#!/usr/bin/env python3
"""backlog-orchestrator — superviseur deterministe AgentBacklog <-> AgentControl.

Boucle (toutes les POLL_S secondes) :
  1. sessions workers (titre "Backlog-W*") non actives :
     - aucune PR ouverte et aucune tache wip -> suppression (acctl rm)
     - sinon -> message ACP de poursuite (merge PR / release tache), cooldown
  2. taches pending + slots libres (< MAX_WORKERS) -> spawn de nouvelles sessions
     (devin, bypass, swe-2-high, thought_level max, groupe AgentControl, /workspace)
  3. taches wip dont l'agent_id est une session disparue -> release au backlog
  4. garde de la release PR (next_release -> main) :
     - backlog vide (ni pending ni wip) + AUCUNE PR ouverte au registre
       + PR draft -> cree l'item "patchnotes + ready" (une fois par cycle
       de travail) qu'un worker executera. Le test PR-ouverte est
       necessaire : les workers marquent --done AVANT le merge de leur
       PR — un item notes genere trop tot raterait les merges tardifs.
     - vrais items pending/wip + PR ready -> repasse en draft (gh pr ready --undo)
       (les items "notes" crees par l'orchestrateur ne comptent pas comme vrai
       travail : c'est eux qui passent la PR ready)

Etat : ~/.agentbacklog/orchestrator-state.json (compteur workers, cooldowns)
Log  : ~/.agentbacklog/orchestrator.log (+ stdout)
Pause : toucher ~/.agentbacklog/orchestrator.pause pour suspendre les actions.

CLI : --once (un cycle), --dry-run (decide sans agir), --status (resume et quitte).
"""

import fcntl
import json
import logging
import os
import re
import subprocess
import sys
import time
import unicodedata
from pathlib import Path

POLL_S = 60
MAX_WORKERS = 5
TITLE_PREFIX = "Backlog-W"
GROUP = "AgentControl"
WORKSPACE = "/workspace"
AGENT = "devin"
MODE = "bypass"
MODEL = "swe-2-high"
NUDGE_COOLDOWN_S = 900  # 15 min entre deux messages a la meme session

STATE_DIR = Path(os.path.expanduser("~/.agentbacklog"))
STATE_FILE = STATE_DIR / "orchestrator-state.json"
LOCK_FILE = STATE_DIR / "orchestrator.lock"
PAUSE_FILE = STATE_DIR / "orchestrator.pause"
LOG_FILE = STATE_DIR / "orchestrator.log"

# Routing workspace : le tag repo d'un item choisit le cwd du worker spawné.
# Surcharge : BACKLOG_REPO_WORKSPACES='{"monrepo": "/chemin"}' (JSON dict).
REPO_WORKSPACES = json.loads(
    os.environ.get("BACKLOG_REPO_WORKSPACES", '{"AgentControl": "/workspace"}'))
DEFAULT_REPO = os.environ.get("BACKLOG_DEFAULT_REPO", "AgentControl")

PROMPT = """[Session creee automatiquement par le service backlog-orchestrator — pas par un agent humain ou une session parente.]

Tu es un worker autonome sur le repo {REPO} (cwd {WS}). Commence par lire {WS}/AGENTS.md s'il est present — le workflow PR/CI qu'il decrit est alors obligatoire.

MISSION — boucle tant que le backlog partage contient des taches :

1. Recupere la prochaine tache : agentbacklog --pop --agent-id "$AGENTCONTROL_SESSION" --repo {REPO} --json (si la variable est vide, utilise un id stable de ton choix et reutilise-le partout). Exit 1 « queue is empty » -> plus de tache disponible : si une de tes PR attend encore un merge, va a l'etape 4, sinon reponds « backlog vide » et arrete. Exit 0 -> le JSON {id, title, description} est ton cahier des charges complet.

   Cas particulier : si une de tes taches reste wip parce que sa PR n'est pas encore mergée, tu n'es PAS libre — mene d'abord cette PR jusqu'au merge (etape 4), marque --done, puis seulement reprends la boucle ici.

2. Implemente la tache en suivant sa description a la lettre : git worktree dedie base sur origin/next_release (JAMAIS dans le checkout principal {WS}), spec, verifications demandees.

3. Cycle PR (cf AGENTS.md du repo) : branche -> push -> PR draft -> review -> iterations en draft -> review soldee -> ready >= 30 s apres le dernier push -> checks requis VERTS sur le head SHA.

4. Mene ta PR jusqu'au merge : si la regle de merge de {WS}/AGENTS.md le permet pour un agent (derniere review auto « Verdict: APPROVED » posterieure au push du head courant, fraicheur verifiee via les PushEvents du repo), procede au merge toi-meme ; sinon le merge reste a l'utilisateur — surveille jusqu'au merge effectif (gh pr view <N> --json state).

5. Marque la tache faite : agentbacklog --done <ID> --agent-id "<ton agent-id>" UNIQUEMENT quand ta PR est effectivement MERGED (verifie l'etat GitHub, pas seulement ready+checks). Pour une tache sans PR propre (ex. item patchnotes auto) : --done quand les criteres de la tache sont remplis.

Blocage insurmontable sur une tache : agentbacklog --release <ID> --agent-id "<ton agent-id>", explique la raison dans ta reponse, puis reprends a l'etape 1.

Regles absolues : ne jamais stopper/redemarrer/toucher le daemon agentcontrol-daemon ; aucun secret commite ; push uniquement sur ta branche de PR ; nettoie ton worktree a la fin (git worktree remove) ; reponds en francais."""

NOTES_TITLE = "[auto] Patchnotes utilisateur de la release PR + passage ready"
NOTES_DESC = """[Item cree automatiquement par backlog-orchestrator : backlog vide, cloture du cycle de release.]

La PR de release (next_release -> main, creee par auto-release-pr.yml — retrouve-la via `gh pr list --head next_release --base main --state open --json number,isDraft`) doit etre prete a etre shippee par l'utilisateur.

MISSION :

1. Body = patchnotes utilisateur EN ANGLAIS couvrant TOUTES les PR mergees dans next_release sur le cycle (inventaire : git fetch origin && git log origin/main..origin/next_release --oneline, ou les commits de la PR). Le generateur automatique (auto-release-pr.yml, cf item #18 / PR #126-127) regenere le body a chaque push sur next_release : verifie qu'il est a jour vis-a-vis du head courant, EN ANGLAIS, et lisible par un utilisateur (fonctionnalites visibles, pas le detail technique interne). S'il est stale ou non anglophone, declenche la regeneration via le mecanisme du workflow ; a defaut reecris le body toi-meme (gh pr edit <N> --body). Si le generateur doit produire de l'anglais durablement, corrige-le via le cycle PR normal plutot qu'une edition manuelle.
2. Passe la PR en ready : gh pr ready <N> — cela declenche release-bump (tag rc.k + commit de version sur next_release), c'est voulu. Ne merge JAMAIS la release PR vers main : le merge reste a l'utilisateur.
3. Si le generateur produit un body defectueux, corrige-le via le cycle PR normal dans un worktree separe (base origin/next_release).
4. Quand la release PR est ready avec un body a jour : agentbacklog --done <ID> --agent-id "<ton agent-id>" ET ARRETE. ATTENTION, tache speciale : n'attends PAS le merge de la release PR — son merge vers main est reserve a l'utilisateur et peut prendre des jours ; la regle generale « --done au merge » ne s'applique pas ici, tes criteres sont : body frais + PR ready. Si tu as produit une PR propre (fix du generateur), celle-la suit la regle normale (merge)."""


def log_setup():
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
    )


def run(cmd, timeout=60, cwd=None):
    try:
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd)
        return r.returncode, r.stdout.strip(), r.stderr.strip()
    except subprocess.TimeoutExpired:
        return -1, "", "timeout"


def acctl(*args):
    rc, out, _ = run(["acctl", *args])
    return rc, out


def acctl_json(*args):
    rc, out = acctl(*args)
    if rc != 0 or not out:
        return None
    try:
        start = out.find("{") if out[0] == "{" else out.find("[")
        return json.loads(out[start:]) if start >= 0 else None
    except (json.JSONDecodeError, IndexError):
        return None


def rpc(payload):
    """Requete NDJSON brute sur le socket TCP du daemon."""
    import socket
    try:
        host, port = os.environ.get("AGENTCONTROL_TCP", "127.0.0.1:7331").split(":")
        with socket.create_connection((host, int(port)), timeout=10) as s:
            f = s.makefile("rw")
            f.write(json.dumps(payload) + "\n")
            f.flush()
            return json.loads(f.readline().strip())
    except Exception as e:
        logging.warning("rpc %s failed: %s", payload.get("req"), e)
        return None


# Badges terminaux d'une ligne `acctl list` : "[#150 #151(draft)]" puis
# "[via <origin>]" — seuls des groupes EN FIN de ligne et de forme badge
# sont retires (un titre contenant "[ux]" ne doit pas etre tronque).
BADGE_TAIL = re.compile(r"(\s+\[(?:c#|#|!|via )[^\]]*\])+$")


def list_sessions():
    rc, out = acctl("list")
    if rc != 0:
        return []
    sessions = []
    for line in out.splitlines():
        parts = line.split()
        if len(parts) >= 2 and parts[0].startswith("s-"):
            sid, state = parts[0], parts[1]
            title = BADGE_TAIL.sub("", " ".join(parts[4:])).strip()
            sessions.append({"id": sid, "state": state, "title": title})
    return sessions


def backlog_tasks():
    rc, out, _ = run(["agentbacklog", "--list", "--all", "--json"])
    if rc != 0 or not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []


def registry_prs():
    data = acctl_json("pr", "ls", "--json")
    return data if isinstance(data, list) else []


def gh_pr_state(pr):
    rc, out, _ = run(
        ["gh", "pr", "view", str(pr), "--json", "state", "-q", ".state"], cwd=WORKSPACE
    )
    return out if rc == 0 and out else "UNKNOWN"


def release_pr():
    """PR de release auto (next_release -> main) si ouverte : {number, isDraft}."""
    rc, out, _ = run(
        ["gh", "pr", "list", "--head", "next_release", "--base", "main",
         "--state", "open", "--json", "number,isDraft", "--limit", "1"],
        cwd=WORKSPACE)
    if rc == 0 and out:
        try:
            lst = json.loads(out)
            return lst[0] if lst else None
        except json.JSONDecodeError:
            pass
    return None


def add_notes_item():
    rc, out, _ = run(
        ["agentbacklog", "--add", NOTES_TITLE, "--desc", NOTES_DESC,
         "--repo", "AgentControl", "--json"])
    if rc != 0 or not out:
        logging.error("notes item add failed rc=%s out=%s", rc, out[:200])
        return None
    try:
        return json.loads(out).get("id")
    except json.JSONDecodeError:
        m = re.findall(r"\d+", out)
        return int(m[-1]) if m else None


def load_state():
    try:
        return json.loads(STATE_FILE.read_text())
    except Exception:
        return {"counter": 0, "nudges": {}}


def save_state(st):
    STATE_FILE.write_text(json.dumps(st, indent=1))


def send_nudge(sid, open_prs, wip_ids, repo=None):
    repo = repo or DEFAULT_REPO
    parts = []
    if open_prs:
        parts.append(
            "Tes PR encore ouvertes : %s. Ta mission continue jusqu'au merge — relis la regle de merge de /workspace/AGENTS.md : si ses criteres sont reunis (verdict APPROVED posterieur au push du head courant, fraicheur verifiee via PushEvents), procede au merge toi-meme ; sinon surveille jusqu'au merge utilisateur."
            % ", ".join("#%d" % p for p in open_prs)
        )
    if wip_ids:
        parts.append(
            "Tes taches encore wip : %s. Si tu les as abandonnees, release-les : agentbacklog --release <ID> --agent-id \"$AGENTCONTROL_SESSION\". Sinon reprends le travail dessus."
            % ", ".join("#%d" % t for t in wip_ids)
        )
    parts.append(
        "Quand tout est solde : agentbacklog --pop --agent-id \"$AGENTCONTROL_SESSION\" --repo %s --json pour la tache suivante s'il en reste, sinon arrete."
        % repo
    )
    msg = " ".join(parts)
    rc, _ = acctl("send", sid, msg)
    if rc != 0:
        rc2, _ = acctl("resume", sid)
        if rc2 == 0:
            rc, _ = acctl("send", sid, msg)
    return rc == 0


def spawn_worker(number, repo, ws):
    """Cree une session racine : le prompt arrive comme message UTILISATEUR
    (pas de PeerMessage attribue a une session parente = pas de faux 'message
    de Manager'). Le groupe est pose apres coup par RPC. Le worker démarre
    dans le workspace du repo de l'item qui a déclenché le spawn."""
    title = "%s%d" % (TITLE_PREFIX, number)
    rc, out = acctl(
        "new", AGENT, ws, PROMPT.replace("{REPO}", repo).replace("{WS}", ws),
        "--title", title, "--mode", MODE, "--model", MODEL,
        # meme provenance que l'orchestrateur natif (COMPONENT_ORIGIN) :
        # badge "via" sur le prompt et rattachement au noeud composant.
        "--origin", "component:backlog-orchestrator",
    )
    sid = None
    if rc == 0 and out:
        # `acctl new` imprime l'id nu (s-xxxx) ; d'autres commandes du JSON.
        if out.startswith("s-"):
            sid = out.split()[0]
        else:
            try:
                sid = json.loads(out).get("session")
            except json.JSONDecodeError:
                sid = None
    if not sid:
        logging.error("spawn %s failed (rc=%s out=%s)", title, rc, out[:200])
        return None
    rpc({"id": 1, "req": "set_session_group", "session": sid, "group": GROUP})
    acctl("setcfg", sid, "thought_level", "max")
    logging.info("spawned %s = %s", title, sid)
    return sid


def _poppable(t):
    # schema actuel : "dependencies" = [{id, status}] — poppable seulement si
    # TOUTES les deps sont done. Fallback vieux schema (depends_on/dep_status)
    # si la clef est absente.
    deps = t.get("dependencies")
    if deps is not None:
        return all(d.get("status") == "done" for d in deps)
    return not t.get("depends_on") or t.get("dep_status") == "done"


def cycle(state, dry):
    tasks = backlog_tasks()
    sessions = list_sessions()
    session_ids = {s["id"] for s in sessions}
    prs = registry_prs()

    workers = [s for s in sessions if s["title"].startswith(TITLE_PREFIX)]
    # pending POPPABLE seulement : un item dep-bloqué ne peut pas etre popé
    # -> il ne doit pas compter pour le spawn, sinon on spawn des workers
    # qui trouvent « queue is empty » (churn spawn/rm).
    pending = [t for t in tasks if t["status"] == "pending" and _poppable(t)]
    wip = [t for t in tasks if t["status"] == "wip"]
    now = time.time()
    state.setdefault("assignments", {})
    state.setdefault("clean_since", {})
    # release PR identifiee tot : elle est exclue partout (ledger + garde
    # patchnotes) — son merge est reserve a l'utilisateur, elle ne doit
    # ni bloquer la creation de l'item notes ni etre attribuee comme
    # "PR a merger" a un worker.
    rpr = release_pr()
    rpr_num = rpr["number"] if rpr else None
    wip_by_agent = {}
    for t in wip:
        aid = t.get("agent_id")
        if aid and aid not in wip_by_agent:
            wip_by_agent[aid] = t

    # 1. taches wip orphelines (agent_id = session disparue) -> release
    for t in wip:
        aid = t.get("agent_id") or ""
        if aid.startswith("s-") and aid not in session_ids:
            logging.info("task #%d orphaned (owner %s gone) -> release", t["id"], aid)
            if not dry:
                run(["agentbacklog", "--release", str(t["id"]), "--agent-id", aid])
            pending.append(t)

    # 2. ledger worker -> {items, prs} + gestion des inactifs
    nudges = state["nudges"]
    for w in list(workers):
        sid, st = w["id"], w["state"]
        # items possedes par ce worker (agent_id persiste apres done)
        my_items = {t["id"]: t["status"] for t in tasks if t.get("agent_id") == sid}
        my_open_items = [i for i, s in my_items.items() if s == "wip"]
        my_done = [i for i, s in my_items.items() if s == "done"]
        my_prs = {p["pr"]: gh_pr_state(p["pr"])
                  for p in prs if p.get("session") == sid and p.get("status") != "closed"}
        # la release PR est exclue des PR "a mener au merge" : merge reserve user
        open_prs = [n for n, s in my_prs.items() if s == "OPEN" and n != rpr_num]
        # ledger visible via --status
        state["assignments"][sid] = {
            "title": w["title"], "state": st,
            "tasks": my_items, "prs": my_prs,
        }
        if st in ("working", "compacting", "starting"):
            state["clean_since"].pop(sid, None)
            continue
        # inactif : delete seulement si TOUT est solde
        if not open_prs and not my_open_items:
            unresolved = bool(my_done) and not my_prs  # items done mais aucune PR enregistree (lag ?)
            since = state["clean_since"].setdefault(sid, now)
            if unresolved and now - since < 240:
                logging.info("%s clean-sauf-PR-non-enregistrees (done=%s) -> grace %ds",
                             sid, my_done, int(now - since))
                continue
            logging.info("%s inactive, items soldes (done=%s), prs=%s -> rm",
                         sid, my_done, my_prs)
            if not dry:
                acctl("rm", sid)
            for k in (nudges, state["clean_since"], state["assignments"]):
                k.pop(sid, None)
            workers.remove(w)
            continue
        state["clean_since"].pop(sid, None)
        if now - nudges.get(sid, 0) >= NUDGE_COOLDOWN_S:
            logging.info("%s idle with open_prs=%s wip=%s -> nudge", sid, open_prs, my_open_items)
            # repo du worker : sa wip courante, sinon celui mémorisé au spawn
            wrepo = (wip_by_agent.get(sid) or {}).get("repo") \
                or state.get("worker_repo", {}).get(sid)
            if not dry and send_nudge(sid, open_prs, my_open_items, wrepo):
                nudges[sid] = now

    # 2b. le titre du worker suit son item courant : "Backlog-W22 #45 · titre"
    # (convention partagee avec l'orchestrateur natif du demon — meme
    # ledger -> meme titre, pas de ping-pong de renommages a la reprise).
    def clean_title(raw):
        t = re.sub(r"\s+", " ", (raw or "").strip())
        # "[" / "]" -> "(" / ")" : se confondraient avec les badges
        # "[#N]" / "[via X]" en fin de ligne acctl list (BADGE_TAIL).
        t = t.replace("[", "(").replace("]", ")")
        # caracteres de controle (ANSI…) : pas d'injection dans les titres.
        t = "".join(c for c in t if unicodedata.category(c) != "Cc")
        return t[:40] + "…" if len(t) > 40 else t

    for w in workers:
        sid = w["id"]
        m = re.match(r"(Backlog-W\d+)", w["title"])
        base = m.group(1) if m else w["title"]
        cur = wip_by_agent.get(sid)
        if cur:
            want = "%s #%d · %s" % (base, cur["id"], clean_title(cur.get("title")))
        else:
            want = base
        # acctl list tronque les titres longs a l'affichage : un titre
        # affiche qui est un prefixe du titre voulu = deja correct.
        if w["title"] != want and not want.startswith(w["title"]):
            logging.info("rename %s : %r -> %r", sid, w["title"], want)
            if not dry:
                rpc({"id": 1, "req": "rename_session", "session": sid, "title": want})
            w["title"] = want

    # 3. spawn pour les pending — routage par workspace : le tag repo de
    # l'item choisit le cwd du worker (non tagge -> repo par defaut ;
    # tag inconnu -> skip, on ne route pas a l'aveugle).
    active = len(workers)
    state.setdefault("worker_repo", {})
    for t in pending:
        if active >= MAX_WORKERS:
            break
        repo = t.get("repo") or DEFAULT_REPO
        ws = REPO_WORKSPACES.get(repo)
        if not ws:
            logging.info("item #%d repo=%r sans workspace connu -> skip", t["id"], repo)
            continue
        state["counter"] += 1
        logging.info("pending item #%d repo=%s ws=%s active=%d -> spawn Backlog-W%d",
                     t["id"], repo, ws, active, state["counter"])
        if dry:
            active += 1
        else:
            sid = spawn_worker(state["counter"], repo, ws)
            if sid:
                state["worker_repo"][sid] = repo
                active += 1

    # 4. garde release PR : notes+ready quand vide, draft quand du travail revient
    notes_ids = set(state.get("notes_items", []))
    active_notes = [t for t in tasks
                    if t["id"] in notes_ids and t["status"] in ("pending", "wip")]
    active_other = [t for t in tasks
                    if t["id"] not in notes_ids and t["status"] in ("pending", "wip")]
    if active_other:
        state["notes_cycle_done"] = False  # nouveau cycle de travail
        if rpr and not rpr["isDraft"]:
            logging.info("release PR #%d ready mais backlog non vide -> repasse draft",
                         rpr["number"])
            if not dry:
                run(["gh", "pr", "ready", str(rpr["number"]), "--undo"], cwd=WORKSPACE)
    elif rpr and rpr["isDraft"] and not active_notes and not state.get("notes_cycle_done"):
        # les workers font --done avant le merge de leur PR : backlog vide ne
        # suffit pas — attendre aussi que toute PR LOCALE du registre soit
        # soldee, sinon les patchnotes rateraient un merge tardif. La release
        # PR elle-meme et les PRs d'autres forges/repos ne comptent pas.
        open_any = [
            p["pr"] for p in prs
            if p.get("forge", "github") == "github"
            and p.get("repo") in (None, "", "Wameuh/AgentControl")
            and p["pr"] != rpr_num
            and gh_pr_state(p["pr"]) == "OPEN"
        ]
        if open_any:
            logging.info("backlog vide mais PRs encore ouvertes %s -> patchnotes en attente",
                         open_any)
        else:
            logging.info("backlog vide + release PR #%d draft -> creation item patchnotes",
                         rpr["number"])
            if not dry:
                nid = add_notes_item()
                if nid:
                    state.setdefault("notes_items", []).append(nid)
                    state["notes_cycle_done"] = True

    # purge des entrees de sessions disparues
    for m in (nudges, state["clean_since"], state["assignments"], state["worker_repo"]):
        for sid in list(m):
            if sid not in session_ids:
                m.pop(sid, None)


def main():
    once = "--once" in sys.argv
    dry = "--dry-run" in sys.argv
    status = "--status" in sys.argv
    log_setup()
    state = load_state()
    lock = None
    if not (status or dry):
        lock = open(LOCK_FILE, "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logging.error("another instance holds the lock, exit")
            sys.exit(1)
    if status:
        tasks = backlog_tasks()
        sessions = [s for s in list_sessions() if s["title"].startswith(TITLE_PREFIX)]
        print("pending:", sum(1 for t in tasks if t["status"] == "pending"),
              "| wip:", sum(1 for t in tasks if t["status"] == "wip"))
        rpr = release_pr()
        print("release PR:", ("#%d %s" % (rpr["number"], "draft" if rpr["isDraft"] else "READY"))
              if rpr else "-",
              "| notes_cycle_done:", bool(state.get("notes_cycle_done")))
        for s in sessions:
            a = state.get("assignments", {}).get(s["id"], {})
            print("  %s %-10s %-8s tasks=%s prs=%s"
                  % (s["id"], s["title"], s["state"],
                     a.get("tasks", "?"), a.get("prs", "?")))
        return
    logging.info("orchestrator start (poll=%ds, max=%d, dry=%s)", POLL_S, MAX_WORKERS, dry)
    while True:
        if PAUSE_FILE.exists():
            logging.info("paused (%s exists)", PAUSE_FILE)
        else:
            try:
                cycle(state, dry)
            except Exception:
                logging.exception("cycle failed")
            if not dry:
                save_state(state)
        if once:
            break
        time.sleep(POLL_S)


if __name__ == "__main__":
    main()
