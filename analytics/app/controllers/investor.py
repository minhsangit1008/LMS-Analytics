from fastapi import APIRouter, Query
from ..services.investor_service import (
    get_investor_overall,
    get_investor_invested_ideas,
    get_investor_per_idea,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/investor-overall")
def investor_overall(investor_id: str = Query(..., description="Investor userId (LMS)")):
    return get_investor_overall(investor_id)


@router.get("/investor-invested-ideas")
def investor_invested_ideas(investor_id: str = Query(..., description="Investor userId (LMS)")):
    return get_investor_invested_ideas(investor_id)


@router.get("/investor-per-idea")
def investor_per_idea(
    investor_id: str = Query(..., description="Investor userId (LMS)"),
    idea_id: str | None = Query(None, description="Filter by idea id"),
    mentor_id: str | None = Query(None, description="Filter by mentor userId"),
    student_id: str | None = Query(None, description="Filter by student userId"),
):
    return get_investor_per_idea(investor_id, idea_id, mentor_id, student_id)
