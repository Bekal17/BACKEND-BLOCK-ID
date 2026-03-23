"""
Content moderation engine: bad word filter + trust score penalty.

Level 1 (minor): censor to ***, -2 score
Level 2+ (severe): reject post + penalty
Uses better-profanity + custom Indonesian/crypto word lists.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List

from backend_blockid.database.repositories import insert_wallet_reason
from backend_blockid.database.score_history import log_score_change
from backend_blockid.blockid_logging import get_logger
import httpx

try:
    from better_profanity import profanity
except ImportError:
    profanity = None

SAFE_WORDS = [
    "reputation", "reputations", "reputable",
    "classic", "assumption", "assumptions",
    "passionate", "compassion", "passion",
    "masculine", "association", "associations",
    "documentation", "communication",
    "scunthorpe", "essex", "sussex", "middlesex",
    "cockatoo", "cockerel", "cocktail", "cocoa",
    "assassin", "harassment", "therapist",
    "mastermind", "fundamentals",
]

if profanity is not None:
    try:
        profanity.add_censor_words([])  # ensure initialized
        # Whitelist safe words by loading custom list without them
        current = set(profanity.CENSOR_WORDSET)
        for word in SAFE_WORDS:
            current.discard(word.lower())
        profanity.CENSOR_WORDSET = current
    except Exception:
        pass

# ============================================================
# INDONESIAN (expanded)
# ============================================================
MINOR_WORDS_ID = [
    "anjir", "anjing", "brengsek", "sialan", "kampret",
    "goblok", "tolol", "idiot", "bodoh", "bego",
    "ngentot", "memek", "kontol", "pepek", "titit",
    "bangsat", "bajingan", "keparat", "bedebah", "kurang ajar",
    "tai", "taik", "bacot", "goblog", "asu", "jancok",
    "cok", "jancuk", "matamu", "dancuk", "ndas",
]

SEVERE_WORDS_ID = [
    "kafir", "bunuh dia", "mati lo", "gantung diri",
    "rug semua", "scam semua", "tipu semua",
    "bakar", "ledakkan", "serang exchange",
]

# ============================================================
# ENGLISH — crypto specific (better-profanity handles general)
# ============================================================
MINOR_WORDS_EN = [
    "dumbass", "moron", "retard", "loser", "scammer",
    "shitcoin", "shitpost", "cuck", "shill",
]

SEVERE_WORDS_EN = [
    "kill yourself", "kys", "go die", "kill all",
    "bomb the", "attack the", "destroy all",
    "rug everyone", "exit scam now",
]

# ============================================================
# HINDI (India — #1 crypto user base)
# ============================================================
MINOR_WORDS_HI = [
    "madarchod", "bhenchod", "chutiya", "gaandu",
    "bhosdike", "harami", "kamina", "kutte",
    "suar", "ullu", "bakwaas", "gadha",
    "मादरचोद", "भेंचोद", "चुतिया", "गांडू",
    "हरामी", "कमीना",
]

SEVERE_WORDS_HI = [
    "maar dalo", "kaat dalo", "jaan se maar",
    "bomb blast karo", "attack karo",
    "मार डालो", "जान से मार",
]

# ============================================================
# VIETNAMESE (Vietnam — #4 globally)
# ============================================================
MINOR_WORDS_VI = [
    "đồ chó", "đồ ngu", "thằng ngu", "con ngu",
    "địt mẹ", "đéo", "cặc", "lồn", "buồi",
    "mẹ kiếp", "chó đẻ", "thằng khốn", "đồ điên",
    "ngu vl", "vcl", "vl", "dmm", "dm",
    "do cho", "thang ngu", "con ngu", "dit me",
]

SEVERE_WORDS_VI = [
    "giết chết", "giết hết", "đánh bom",
    "tấn công sàn", "rug hết",
    "giet chet", "danh bom", "tan cong san",
]

# ============================================================
# PORTUGUESE/BRAZILIAN (Brazil — #5 globally)
# ============================================================
MINOR_WORDS_PT = [
    "porra", "caralho", "merda", "fodase", "foda-se",
    "viado", "buceta", "pau", "puta", "filho da puta",
    "fdp", "vai se foder", "vsf", "arrombado",
    "otario", "otário", "idiota", "babaca", "vacilão",
    "corno", "desgraça", "lixo",
]

SEVERE_WORDS_PT = [
    "se mata", "vai morrer", "matar todos",
    "bomb", "atacar exchange", "rug todos",
    "morra", "vai morrer", "te matar",
]

# ============================================================
# TAGALOG/FILIPINO (Philippines — #9, strong Solana ecosystem)
# ============================================================
MINOR_WORDS_TL = [
    "putangina", "putang ina", "puta", "gago",
    "bobo", "tanga", "ulol", "inutil", "pakyu",
    "pakinggan mo", "tarantado", "hayop",
    "leche", "lintik", "pakshet", "pesteng yawa",
    "ptangina", "pota", "p*ta", "gagu",
    "bwisit", "amputa", "yawa",
]

SEVERE_WORDS_TL = [
    "patayin", "papatayin kita", "patayin lahat",
    "bomba", "atake", "rug lahat",
    "mamatay ka", "sige mamatay",
]

# ============================================================
# COMBINED LISTS (used in check_content)
# ============================================================
ALL_MINOR_WORDS = (
    MINOR_WORDS_ID
    + MINOR_WORDS_EN
    + MINOR_WORDS_HI
    + MINOR_WORDS_VI
    + MINOR_WORDS_PT
    + MINOR_WORDS_TL
)

ALL_SEVERE_WORDS = (
    SEVERE_WORDS_ID
    + SEVERE_WORDS_EN
    + SEVERE_WORDS_HI
    + SEVERE_WORDS_VI
    + SEVERE_WORDS_PT
    + SEVERE_WORDS_TL
)

# ============================================================
# CRYPTO-SPECIFIC SCAM PHRASES (all languages)
# ============================================================
CRYPTO_SCAM_PHRASES = [
    "send me your private key", "send your seed phrase",
    "double your bitcoin", "guaranteed profit",
    "dm me for profit", "100x guaranteed",
    "kirim private key", "kirim seed phrase",
    "gandakan bitcoin", "profit dijamin",
    "gửi private key", "gửi seed phrase",
    "nhân đôi bitcoin",
    "envie sua chave privada", "envie seed phrase",
    "dobrar bitcoin", "lucro garantido",
    "ipadala private key", "ipadala seed phrase",
    "doblehin bitcoin",
    "private key bhejo", "seed phrase bhejo",
]

# INCITEMENT patterns — CRITICAL only when combined with action words
INCITEMENT_PATTERNS = [
    ("terrorism", ["lakukan", "execute", "attack on", "bomb", "serang", "hancurkan"]),
    ("jihad", ["serang", "bunuh", "hancurkan", "lawan", "attack"]),
    ("serangan", ["lakukan", "rencanakan", "execute", "besok", "sekarang"]),
    ("bunuh", ["dia", "mereka", "kalian", "semua", "lo", "kamu"]),
    ("ancam", ["akan", "bakal", "mau", "siap"]),
]

# Safe context keywords — if present, downgrade severity
SAFE_CONTEXT_KEYWORDS = [
    "berita", "news", "breaking", "laporan", "report",
    "terjadi", "happened", "kejadian", "incident",
    "anti", "counter", "prevent", "cegah", "lawan",
    "analisis", "analysis", "riset", "research", "study",
    "blockchain", "crypto", "chainalysis", "tracking",
    "thread", "edukasi", "education", "awareness",
    "sejarah", "history", "documentary",
]

# CRITICAL words with NO safe context (zero tolerance)
ABSOLUTE_CRITICAL = [
    "child porn", "cp porn", "underage sex",
    "konten anak", "foto anak telanjang",
]

# Established user thresholds (gets one level grace when safe context present)
ESTABLISHED_USER = {
    "min_trust_score": 70,
    "min_wallet_age_days": 365,
    "min_post_count": 50,
}

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENAI_MODERATION_URL = "https://api.openai.com/v1/moderations"
OPENAI_SCORE_THRESHOLD = float(os.getenv("OPENAI_SCORE_THRESHOLD", "0.75") or "0.75")

OPENAI_CATEGORY_LEVELS: dict[str, int] = {
    # Level 4 CRITICAL
    "sexual/minors": 4,
    "violence/graphic": 4,
    # Level 3 SEVERE
    "hate": 3,
    "harassment/threatening": 3,
    "violence": 3,
    "self-harm/intent": 3,
    "illicit/violent": 3,
    # Level 2 MODERATE
    "sexual": 2,
    "self-harm": 2,
    "harassment": 2,
    "self-harm/instructions": 2,
    "illicit": 2,
    "hate/threatening": 2,
}

LEVEL_MINOR = 1
LEVEL_MODERATE = 2
LEVEL_SEVERE = 3
LEVEL_CRITICAL = 4

PENALTY = {1: -2, 2: -5, 3: -15, 4: -50}
ACTION_TAKEN = {1: "CENSORED", 2: "REJECTED", 3: "REJECTED", 4: "BLOCKED"}

violation_logger = logging.getLogger("blockid.violations")
logger = get_logger(__name__)
_CRITICAL_LOG_PATH = Path("logs/critical_violations.log")


def detect_language_hint(text: str) -> str:
    """
    Simple heuristic to detect likely language for logging/analytics only.
    NOT used for moderation decisions.
    """
    text_lower = (text or "").lower()
    if any(w in text_lower for w in MINOR_WORDS_VI + SEVERE_WORDS_VI):
        return "vi"
    if any(w in text_lower for w in MINOR_WORDS_TL + SEVERE_WORDS_TL):
        return "tl"
    if any(w in text_lower for w in MINOR_WORDS_PT + SEVERE_WORDS_PT):
        return "pt"
    if any(w in text_lower for w in MINOR_WORDS_HI + SEVERE_WORDS_HI):
        return "hi"
    if any(w in text_lower for w in MINOR_WORDS_ID + SEVERE_WORDS_ID):
        return "id"
    return "en"


def _contains_any(text: str, words: List[str]) -> List[str]:
    """Case-insensitive substring match; return list of matched words."""
    text_lower = (text or "").lower()
    found: List[str] = []
    for w in words:
        if w.lower() in text_lower:
            found.append(w)
    return found


def _censor_words(text: str, words: List[str]) -> str:
    """Replace each occurrence of given words (case-insensitive) with ***."""
    out = text
    for w in words:
        # Simple replace, case-insensitive by scanning
        lower = out.lower()
        idx = 0
        while True:
            i = lower.find(w.lower(), idx)
            if i < 0:
                break
            out = out[:i] + "***" + out[i + len(w):]
            lower = out.lower()
            idx = i + 3
    return out


# TEST CASES (do not remove — for regression testing):
#
# Should BLOCK (level 4):
# "lakukan terrorism attack pada exchange besok"
# "foto anak telanjang"
#
# Should REJECT (level 3) — incitement with safe context:
# "Breaking news: terrorism attack terjadi di Jakarta, korban 10 orang"
# → contains "terrorism" + "attack" but has safe context "breaking news"
#
# Should REJECT (level 2) — severe word no context:
# "fuck semua orang di sini"
#
# Should ALLOW_CENSORED (level 1):
# "anjir wallet ini bagus banget"
# → becomes "***** wallet ini bagus banget"
#
# Should ALLOW (level 0):
# "Analisis: terrorism financing melalui blockchain meningkat 40%"
# → has safe context, no incitement action word
# "Thread tentang counter terrorism dan crypto tracking"
# → educational content


async def check_openai_moderation(text: str) -> Dict[str, Any]:
    """
    Call OpenAI Moderation API and map to our violation levels.
    Always returns a dict; never raises.
    """
    if not OPENAI_API_KEY:
        return {
            "flagged": False,
            "violation_level": 0,
            "available": False,
            "categories": [],
            "scores": {},
            "action": "ALLOW",
        }

    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.post(
                OPENAI_MODERATION_URL,
                headers={
                    "Authorization": f"Bearer {OPENAI_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={"input": text},
            )
            response.raise_for_status()
            data = response.json()

        result = data["results"][0]
        flagged = bool(result.get("flagged"))
        categories = result.get("categories") or {}
        scores = result.get("category_scores") or {}

        max_level = 0
        triggered_categories: List[str] = []
        for category, is_flagged in categories.items():
            score = float(scores.get(category, 0) or 0)
            if is_flagged or score >= OPENAI_SCORE_THRESHOLD:
                level = OPENAI_CATEGORY_LEVELS.get(category, 2)
                if level > max_level:
                    max_level = level
                triggered_categories.append(f"{category}({score:.2f})")

        if max_level == 0:
            action = "ALLOW"
        elif max_level <= LEVEL_MODERATE:
            action = "REJECT"
        elif max_level == LEVEL_SEVERE:
            action = "REJECT"
        else:
            action = "BLOCK"

        return {
            "flagged": flagged or max_level > 0,
            "violation_level": max_level,
            "categories": triggered_categories,
            "scores": scores,
            "action": action,
            "available": True,
        }
    except httpx.TimeoutException:
        violation_logger.warning("openai_moderation_timeout", text_length=len(text or ""))
        return {
            "flagged": False,
            "violation_level": 0,
            "available": False,
            "categories": [],
            "scores": {},
            "action": "ALLOW",
        }
    except Exception as e:  # pragma: no cover - best-effort logging
        violation_logger.warning("openai_moderation_error", error=str(e))
        return {
            "flagged": False,
            "violation_level": 0,
            "available": False,
            "categories": [],
            "scores": {},
            "action": "ALLOW",
        }


async def check_content(
    text: str,
    trust_score: float = 0,
    wallet_age_days: int = 0,
    post_count: int = 0,
) -> Dict[str, Any]:
    """
    Context-aware content check.
    Detection order: ABSOLUTE_CRITICAL → INCITEMENT_PATTERNS → SEVERE → profanity → MINOR.
    Established user grace (only with safe context): level 4→3, 3→2; never below LEVEL_MODERATE.
    """
    text = (text or "").strip()
    text_lower = (text or "").lower()
    violations: List[str] = []
    default_extra = {"context": "CLEAN", "downgraded": False}

    # Step 1: ABSOLUTE_CRITICAL — zero tolerance
    for word in ABSOLUTE_CRITICAL:
        if word in text_lower:
            return {
                "is_clean": False,
                "violation_level": LEVEL_CRITICAL,
                "cleaned_text": text,
                "violations": [word],
                "action": "BLOCK",
                "context": "ABSOLUTE_CRITICAL",
                "downgraded": False,
                "layer": 1,
            }

    # Step 2: Check safe context presence
    has_safe_context = any(kw in text_lower for kw in SAFE_CONTEXT_KEYWORDS)

    # Step 3: Check incitement patterns
    for trigger_word, action_words in INCITEMENT_PATTERNS:
        if trigger_word in text_lower:
            has_action = any(aw in text_lower for aw in action_words)
            if has_action:
                level = LEVEL_CRITICAL
                context = "INCITEMENT"
                if has_safe_context:
                    level = LEVEL_SEVERE
                    context = "INCITEMENT_WITH_SAFE_CONTEXT"
                is_established = (
                    trust_score >= ESTABLISHED_USER["min_trust_score"]
                    and wallet_age_days >= ESTABLISHED_USER["min_wallet_age_days"]
                    and post_count >= ESTABLISHED_USER["min_post_count"]
                )
                downgraded = False
                if is_established and has_safe_context:
                    level = max(LEVEL_MODERATE, level - 1)
                    downgraded = True
                action = "BLOCK" if level == LEVEL_CRITICAL else "REJECT"
                return {
                    "is_clean": False,
                    "violation_level": level,
                    "cleaned_text": text,
                    "violations": [trigger_word],
                    "action": action,
                    "context": context,
                    "downgraded": downgraded,
                    "layer": 1,
                }

    # Step 3.5: Crypto scam phrases → always SEVERE (level 3)
    for phrase in CRYPTO_SCAM_PHRASES:
        if phrase in text_lower:
            return {
                "is_clean": False,
                "violation_level": LEVEL_SEVERE,
                "cleaned_text": text,
                "violations": [phrase],
                "action": "REJECT",
                "context": "CRYPTO_SCAM",
                "downgraded": False,
                "layer": 1,
            }

    # Step 4: SEVERE combined list → level 2+ → REJECT
    severe_found = _contains_any(text, ALL_SEVERE_WORDS)
    if severe_found:
        violations.extend(severe_found)
        return {
            "is_clean": False,
            "violation_level": LEVEL_MODERATE if len(severe_found) == 1 else LEVEL_SEVERE,
            "cleaned_text": text,
            "violations": violations,
            "action": "REJECT",
            "context": "SEVERE_WORD",
            "downgraded": False,
            "layer": 1,
        }

    # Step 5: better-profanity (English) severe
    if profanity is not None and profanity.contains_profanity(text):
        violations.append("profanity_en")
        return {
            "is_clean": False,
            "violation_level": LEVEL_MODERATE,
            "cleaned_text": text,
            "violations": violations,
            "action": "REJECT",
            "context": "PROFANITY_SEVERE",
            "downgraded": False,
            "layer": 2,
        }

    # Step 6: Minor combined list → level 1 → ALLOW_CENSORED
    minor_found = _contains_any(text, ALL_MINOR_WORDS)
    if minor_found:
        cleaned = _censor_words(text, minor_found)
        return {
            "is_clean": False,
            "violation_level": LEVEL_MINOR,
            "cleaned_text": cleaned,
            "violations": minor_found,
            "action": "ALLOW_CENSORED",
            "context": "MINOR_WORD",
            "downgraded": False,
            "layer": 1,
        }

    # Step 7: better-profanity mild → level 1
    if profanity is not None and profanity.contains_profanity(text):
        cleaned = profanity.censor(text)
        return {
            "is_clean": False,
            "violation_level": LEVEL_MINOR,
            "cleaned_text": cleaned,
            "violations": ["profanity_en"],
            "action": "ALLOW_CENSORED",
            "context": "PROFANITY",
            "downgraded": False,
            "layer": 2,
        }

    # Layer 3: OpenAI Moderation API (if available)
    openai_result = await check_openai_moderation(text)
    if openai_result.get("available") and openai_result.get("flagged"):
        level = int(openai_result.get("violation_level") or 0)
        action = openai_result.get("action") or "REJECT"
        is_established = (
            trust_score >= ESTABLISHED_USER["min_trust_score"]
            and wallet_age_days >= ESTABLISHED_USER["min_wallet_age_days"]
            and post_count >= ESTABLISHED_USER["min_post_count"]
        )
        downgraded = False
        if is_established and level >= LEVEL_SEVERE:
            level = max(LEVEL_MODERATE, level - 1)
            downgraded = True
            if level < LEVEL_CRITICAL:
                action = "REJECT"

        return {
            "is_clean": False,
            "violation_level": level,
            "cleaned_text": text,
            "violations": openai_result.get("categories") or [],
            "action": action,
            "context": "OPENAI_MODERATION",
            "downgraded": downgraded,
            "layer": 3,
            "openai_scores": openai_result.get("scores") or {},
        }

    # All layers passed — clean
    return {
        "is_clean": True,
        "violation_level": 0,
        "cleaned_text": text,
        "violations": [],
        "action": "ALLOW",
        "context": "CLEAN",
        "downgraded": False,
        "layer": 0,
    }


async def log_violation(
    wallet: str,
    content: str,
    violation_level: int,
    conn,
) -> None:
    """Log content violation to content_violations table. Non-blocking. Includes language hint for analytics."""
    try:
        preview = (content or "")[:100]
        action = ACTION_TAKEN.get(violation_level, "REJECTED")
        penalty = PENALTY.get(violation_level, 0)
        lang = detect_language_hint(content or "")
        try:
            await conn.execute(
                """
                INSERT INTO content_violations
                (wallet, content_preview, violation_level, action_taken, trust_penalty, language, created_at)
                VALUES ($1, $2, $3, $4, $5, $6, NOW())
                """,
                wallet,
                preview,
                violation_level,
                action,
                abs(penalty),
                lang,
            )
        except Exception as col_err:
            if "language" in str(col_err).lower() or "column" in str(col_err).lower():
                await conn.execute(
                    """
                    INSERT INTO content_violations
                    (wallet, content_preview, violation_level, action_taken, trust_penalty, created_at)
                    VALUES ($1, $2, $3, $4, $5, NOW())
                    """,
                    wallet,
                    preview,
                    violation_level,
                    action,
                    abs(penalty),
                )
            else:
                raise
    except Exception as e:
        violation_logger.debug("log_violation_failed", wallet=wallet[:16], error=str(e))


def _reason_code_for_level(level: int) -> str | None:
    if level == 2:
        return "CONTENT_VIOLATION_MODERATE"
    if level == 3:
        return "CONTENT_VIOLATION_SEVERE"
    if level == 4:
        return "CONTENT_VIOLATION_CRITICAL"
    if level == 1:
        return "CONTENT_VIOLATION_MINOR"
    return None


async def apply_content_penalty(
    wallet: str,
    violation_level: int,
    conn,
) -> Dict[str, Any]:
    """
    Apply trust score penalty and posting restrictions.
    Returns penalty_applied, trust_score_delta, new_trust_score,
    suspended_until, permanently_disabled, violation_level.
    """
    wallet = (wallet or "").strip()
    if not wallet or violation_level < 1:
        return {
            "penalty_applied": False,
            "trust_score_delta": 0,
            "new_trust_score": None,
            "suspended_until": None,
            "permanently_disabled": False,
            "violation_level": violation_level,
        }

    delta = PENALTY.get(violation_level, 0)
    now = datetime.utcnow()

    # Update trust_scores (score column, floor 0, cap 97)
    row = await conn.fetchrow(
        "SELECT score, risk_level FROM trust_scores WHERE wallet = $1",
        wallet,
    )
    current = float(row["score"]) if row and row["score"] is not None else 0.0
    risk_level_before = row["risk_level"] if row and row["risk_level"] is not None else None
    new_score = max(0.0, min(97.0, current + delta))

    # Score history hook (MODERATION) — non-fatal, BEFORE UPDATE
    reason_code = _reason_code_for_level(violation_level)
    try:
        logger.info(
            "score_history_moderation_hook",
            wallet=wallet[:16],
            violation_level=violation_level,
            score_before=current,
        )
        await log_score_change(
            wallet=wallet,
            score_before=current,
            score_after=new_score,
            change_category="MODERATION",
            triggered_by="moderation_engine",
            reason_codes=[reason_code] if reason_code else None,
            violation_level=int(violation_level),
            risk_level=str(risk_level_before) if risk_level_before is not None else None,
            metadata=None,
        )
    except Exception:  # pragma: no cover - best-effort
        pass

    await conn.execute(
        """
        UPDATE trust_scores
        SET score = $1, updated_at = NOW()
        WHERE wallet = $2
        """,
        new_score,
        wallet,
    )

    # Insert reason code via repository (handles duplicate safely)
    if reason_code:
        await insert_wallet_reason(
            wallet=wallet,
            reason_code=reason_code,
            weight=delta,
            confidence=1.0,
        )

    suspended_until = None
    permanently_disabled = False

    if violation_level == 2:
        restricted_until = now + timedelta(days=7)
        await conn.execute(
            """
            INSERT INTO posting_restrictions
            (wallet, restriction_type, posts_per_day, restricted_until, reason, updated_at)
            VALUES ($1, 'RATE_LIMITED', 3, $2, 'CONTENT_VIOLATION_MODERATE', NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                restriction_type = 'RATE_LIMITED',
                posts_per_day = 3,
                restricted_until = $2,
                reason = 'CONTENT_VIOLATION_MODERATE',
                updated_at = NOW()
            """,
            wallet,
            restricted_until,
        )
        suspended_until = restricted_until

    if violation_level == 3:
        restricted_until = now + timedelta(days=30)
        await conn.execute(
            """
            INSERT INTO posting_restrictions
            (wallet, restriction_type, posts_per_day, restricted_until, reason, updated_at)
            VALUES ($1, 'SUSPENDED', 0, $2, 'CONTENT_VIOLATION_SEVERE', NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                restriction_type = 'SUSPENDED',
                posts_per_day = 0,
                restricted_until = $2,
                reason = 'CONTENT_VIOLATION_SEVERE',
                updated_at = NOW()
            """,
            wallet,
            restricted_until,
        )
        suspended_until = restricted_until

    if violation_level == 4:
        await conn.execute(
            """
            INSERT INTO posting_restrictions
            (wallet, restriction_type, posts_per_day, restricted_until, reason, updated_at)
            VALUES ($1, 'PERMANENT', 0, NULL, 'CONTENT_VIOLATION_CRITICAL', NOW())
            ON CONFLICT (wallet) DO UPDATE SET
                restriction_type = 'PERMANENT',
                posts_per_day = 0,
                restricted_until = NULL,
                reason = 'CONTENT_VIOLATION_CRITICAL',
                updated_at = NOW()
            """,
            wallet,
        )
        permanently_disabled = True
        violation_logger.critical(
            "CRITICAL_VIOLATION wallet=%s level=4 action=PERMANENT_DISABLE timestamp=%s",
            wallet[:16],
            now.isoformat(),
        )
        try:
            _CRITICAL_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_CRITICAL_LOG_PATH, "a", encoding="utf-8") as f:
                f.write(
                    f"{now} | CRITICAL | {wallet} | permanent disabled\n"
                )
        except Exception as e:
            violation_logger.warning("critical_log_file_failed", error=str(e))

    return {
        "penalty_applied": True,
        "trust_score_delta": delta,
        "new_trust_score": new_score,
        "suspended_until": suspended_until,
        "permanently_disabled": permanently_disabled,
        "violation_level": violation_level,
    }
