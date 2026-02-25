from fastapi import APIRouter, Query
from ..services.mentor_service import get_mentor_overall, get_mentor_per_idea

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/mentor-overall")
def mentor_overall(mentor_id: int = Query(..., description="Moodle mentor user id")):
    return get_mentor_overall(mentor_id)


@router.get("/mentor-per-idea")
def mentor_per_idea(
    mentor_id: int = Query(..., description="Moodle mentor user id"),
    idea_id: str | None = Query(None, description="Idea id"),
):
    return get_mentor_per_idea(mentor_id, idea_id)
