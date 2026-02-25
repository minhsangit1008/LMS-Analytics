from fastapi import HTTPException
from sqlalchemy import text

from ..db import LMS_ENGINE
from ..routers.common import _fmt_dt


def _pitch_score(status: str | None, funding: float | None) -> float:
    base = 50
    if status == "approve":
        base = 80
    elif status == "reject":
        base = 20
    bonus = min(20, (funding or 0) / 1000) if funding else 0
    return round(min(100, base + bonus), 1)




def get_investor_overall(investor_id: str):
    with LMS_ENGINE.connect() as conn:
        pitch_total = conn.execute(
            text("SELECT COUNT(*) AS c FROM pitchperfect WHERE investorId = :iid"),
            {"iid": investor_id},
        ).scalar()
        funding_total = conn.execute(
            text("SELECT SUM(funding) AS s FROM pitchperfect WHERE investorId = :iid"),
            {"iid": investor_id},
        ).scalar()

        upcoming_pitches = conn.execute(
            text(
                """
                SELECT COUNT(*) AS c
                FROM pitchperfect
                WHERE eventDate IS NOT NULL
                  AND investorId = :iid
                  AND eventDate >= UTC_TIMESTAMP()
                  AND eventDate <= DATE_ADD(UTC_TIMESTAMP(), INTERVAL 7 DAY)
                """
            ),
            {"iid": investor_id},
        ).scalar()

        pitch_rows = conn.execute(
            text(
                """
                SELECT p.ideaId, p.status, p.funding, p.eventDate, b.name, b.status AS ideaStatus, b.tags
                FROM pitchperfect p
                LEFT JOIN businessidea b ON b.id = p.ideaId
                WHERE p.investorId = :iid
                """
            ),
            {"iid": investor_id},
        ).mappings().all()

        domain_rows = conn.execute(
            text(
                """
                SELECT
                  CASE
                    WHEN b.tags IS NULL OR b.tags = '' THEN 'unknown'
                    ELSE SUBSTRING_INDEX(b.tags, ',', 1)
                  END AS domain,
                  COUNT(*) AS c
                FROM businessidea b
                JOIN pitchperfect p ON p.ideaId = b.id
                WHERE p.investorId = :iid
                GROUP BY domain
                """
            ),
            {"iid": investor_id},
        ).mappings().all()

    scores = [
        _pitch_score(r.get("status"), float(r.get("funding") or 0))
        for r in pitch_rows
    ]
    top_ideas = []
    progress_map = {}
    try:
        with LMS_ENGINE.connect() as conn:
            prog_rows = conn.execute(
                text(
                    """
                    SELECT uwi.instanceId, uwi.completionPercentage
                    FROM userworkflowinstance uwi
                    WHERE uwi.instanceId IS NOT NULL
                    """
                )
            ).mappings().all()
        for r in prog_rows:
            idea_id = r.get("instanceId")
            if not idea_id:
                continue
            progress_map[idea_id] = max(
                int(progress_map.get(idea_id, 0)),
                int(r.get("completionPercentage") or 0),
            )
    except Exception:
        progress_map = {}

    for r in pitch_rows:
        score = _pitch_score(r.get("status"), float(r.get("funding") or 0))
        top_ideas.append(
            {
                "ideaId": r.get("ideaId"),
                "ideaName": r.get("name"),
                "ideaStatus": r.get("ideaStatus"),
                "domain": (r.get("tags") or "unknown").split(",")[0],
                "pitchStatus": r.get("status"),
                "funding": float(r.get("funding") or 0),
                "eventDate": _fmt_dt(r.get("eventDate")) if r.get("eventDate") else None,
                "pitchScore": score,
                "progressPercent": int(progress_map.get(r.get("ideaId"), 0)),
            }
        )
    top_ideas = sorted(top_ideas, key=lambda x: x["pitchScore"], reverse=True)[:50]
    ready_to_invest = [i for i in top_ideas if i["pitchScore"] >= 80]
    invested_ideas = [
        i for i in top_ideas if i["pitchStatus"] == "approve" and i["funding"] > 0
    ]
    new_ideas = [
        i for i in top_ideas if not (i["pitchStatus"] == "approve" and i["funding"] > 0)
    ]

    if not pitch_rows:
        raise HTTPException(status_code=404, detail="investor_id not found")

    return {
        "investorId": investor_id,
        "pitchTotal": int(pitch_total or 0),
        "fundingTotal": float(funding_total or 0),
        "upcomingPitches7d": int(upcoming_pitches or 0),
        "readyToInvest": len(ready_to_invest),
        "investedIdeas": invested_ideas,
        "newIdeas": new_ideas,
        "rankingTable": top_ideas,
        "ideaByDomain": {r["domain"]: int(r["c"] or 0) for r in domain_rows},
    }


def get_investor_invested_ideas(investor_id: str):
    with LMS_ENGINE.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT p.ideaId, p.funding, p.status, p.eventDate,
                       b.name, b.status AS ideaStatus, b.tags
                FROM pitchperfect p
                JOIN businessidea b ON b.id = p.ideaId
                WHERE p.investorId = :iid
                  AND p.status = 'approve'
                  AND p.funding IS NOT NULL AND p.funding > 0
                ORDER BY p.eventDate DESC
                """
            ),
            {"iid": investor_id},
        ).mappings().all()

    ideas = [
        {
            "ideaId": r["ideaId"],
            "ideaName": r["name"],
            "ideaStatus": r["ideaStatus"],
            "domain": (r.get("tags") or "unknown").split(",")[0],
            "pitchStatus": r["status"],
            "funding": float(r["funding"] or 0),
            "eventDate": _fmt_dt(r.get("eventDate")) if r.get("eventDate") else None,
        }
        for r in rows
    ]

    return {
        "investorId": investor_id,
        "totalInvested": len(ideas),
        "ideas": ideas,
    }


def get_investor_per_idea(investor_id: str, idea_id: str | None = None, mentor_id: str | None = None, student_id: str | None = None):
    with LMS_ENGINE.connect() as conn:
        rows = conn.execute(
            text(
                """
                SELECT
                  b.id AS ideaId,
                  b.name AS ideaName,
                  b.status AS ideaStatus,
                  b.authorId AS studentId,
                  p.status AS pitchStatus,
                  p.funding AS funding,
                  p.eventDate AS eventDate,
                  m.mentorId AS mentorId,
                  m.dueDate AS dueDate,
                  a.username AS studentName,
                  am.username AS mentorName
                FROM businessidea b
                LEFT JOIN pitchperfect p ON p.ideaId = b.id
                LEFT JOIN studentmentormatch m ON m.ideaId = b.id
                LEFT JOIN account a ON a.userId = b.authorId
                LEFT JOIN account am ON am.userId = m.mentorId
                WHERE p.investorId = :iid
                """
            ),
            {"iid": investor_id},
        ).mappings().all()

    items = []
    for r in rows:
        if idea_id and r.get("ideaId") != idea_id:
            continue
        if mentor_id and r.get("mentorId") != mentor_id:
            continue
        if student_id and r.get("studentId") != student_id:
            continue
        score = _pitch_score(r.get("pitchStatus"), float(r.get("funding") or 0))
        event_date = _fmt_dt(r.get("eventDate")) if r.get("eventDate") else None
        due_date = r.get("dueDate")
        due_iso = _fmt_dt(due_date) if due_date else None
        items.append(
            {
                "ideaId": r.get("ideaId"),
                "ideaName": r.get("ideaName"),
                "ideaStatus": r.get("ideaStatus"),
                "student": {
                    "userId": r.get("studentId"),
                    "name": r.get("studentName"),
                },
                "mentor": {
                    "userId": r.get("mentorId"),
                    "name": r.get("mentorName"),
                },
                "pitch": {
                    "status": r.get("pitchStatus"),
                    "funding": float(r.get("funding") or 0),
                    "eventDate": event_date,
                    "score": score,
                },
                "match": {
                    "dueDate": due_iso,
                },
            }
        )

    if not items:
        raise HTTPException(status_code=404, detail="No idea found for investor")

    return {"ideas": items}
