from datetime import datetime, timedelta
from fastapi import HTTPException

from ..routers.common import _get_lms_user_id, _mentor_build_rows


def get_mentor_overall(mentor_id: int):
    mentor_lms_id = _get_lms_user_id(mentor_id)
    rows = _mentor_build_rows(mentor_lms_id)
    if not rows:
        raise HTTPException(status_code=404, detail="mentor_id not found")

    total_mentees = len({r["student"]["lmsUserId"] for r in rows if r.get("student")})
    total_ideas = len({r["idea"]["id"] for r in rows if r.get("idea")})
    avg_progress = (
        sum(r["progressPct"] for r in rows) / total_mentees if total_mentees else 0
    )
    avg_grade = (
        sum(r["avgGradePct"] for r in rows) / total_mentees if total_mentees else 0
    )

    today = datetime.utcnow().date()
    overdue = 0
    upcoming_7d = 0
    for r in rows:
        if r["dueDate"]:
            try:
                due_date = datetime.fromisoformat(r["dueDate"]).date()
                if due_date < today:
                    overdue += 1
                if 0 <= (due_date - today).days <= 7:
                    upcoming_7d += 1
            except ValueError:
                pass

    deal_ready = [
        r for r in rows if r.get("pitchScore") is not None and r["pitchScore"] >= 80
    ]

    cutoff = datetime.utcnow() - timedelta(days=7)
    new_ideas = []
    for r in rows:
        created_at = r.get("matchCreatedAt")
        if not created_at:
            continue
        try:
            created_dt = datetime.fromisoformat(created_at)
        except ValueError:
            continue
        if created_dt >= cutoff:
            new_ideas.append(r)

    ideas_table = [
        {
            "ideaId": r["idea"]["id"],
            "ideaName": r["idea"].get("name"),
            "ideaStatus": r["idea"].get("status"),
            "studentId": r["student"].get("lmsUserId"),
            "studentName": r["student"].get("name"),
            "pitchStatus": r.get("pitchStatus"),
        }
        for r in rows
    ]

    new_ideas_table = [
        {
            "ideaId": r["idea"]["id"],
            "ideaName": r["idea"].get("name"),
            "ideaStatus": r["idea"].get("status"),
            "studentId": r["student"].get("lmsUserId"),
            "studentName": r["student"].get("name"),
        }
        for r in new_ideas
    ]

    ready_to_invest_table = [
        {
            "ideaId": r["idea"]["id"],
            "ideaName": r["idea"].get("name"),
            "ideaStatus": r["idea"].get("status"),
            "studentId": r["student"].get("lmsUserId"),
            "studentName": r["student"].get("name"),
            "pitchScore": r.get("pitchScore"),
            "pitchStatus": r.get("pitchStatus"),
            "pitchEventDate": r.get("pitchEventDate"),
        }
        for r in deal_ready
    ]

    my_mentoring_table = [
        {
            "ideaId": r["idea"]["id"],
            "ideaName": r["idea"].get("name"),
            "process": r.get("status"),
            "progressPercent": r.get("progressPct"),
        }
        for r in rows
    ]

    return {
        "mentor_id": mentor_id,
        "total_ideas": total_ideas,
        "total_mentees": total_mentees,
        "avg_progress_pct": round(avg_progress, 1),
        "avg_grade_pct": round(avg_grade, 1),
        "overdue_actions": overdue,
        "upcoming_deadlines_7d": upcoming_7d,
        "deal_ready_ideas": len(deal_ready),
        "new_ideas": len(new_ideas),
        "ideas_table": ideas_table,
        "new_ideas_table": new_ideas_table,
        "ready_to_invest_table": ready_to_invest_table,
        "my_mentoring_table": my_mentoring_table,
    }


def get_mentor_per_idea(mentor_id: int, idea_id: str | None = None):
    mentor_lms_id = _get_lms_user_id(mentor_id)
    rows = _mentor_build_rows(mentor_lms_id)
    if not rows:
        raise HTTPException(status_code=404, detail="mentor_id not found")

    if idea_id:
        rows = [r for r in rows if r["idea"].get("id") == idea_id]
        if not rows:
            raise HTTPException(status_code=404, detail="idea_id not found for mentor")

    items = []
    for r in rows:
        items.append(
            {
                "student_userid": r["student"].get("lmsUserId"),
                "fullname": r["student"].get("name"),
                "idea_id": r["idea"].get("id"),
                "idea_name": r["idea"].get("name"),
                "progress_percent": r.get("progressPct"),
                "pitch_score": r.get("pitchScore"),
                "pitch_status": r.get("pitchStatus"),
                "idea_status": r["idea"].get("status"),
            }
        )

    return {
        "mentor_id": mentor_id,
        "ideas": items,
    }
