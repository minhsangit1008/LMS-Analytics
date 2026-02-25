from fastapi import APIRouter, Query
from ..services.teacher_service import get_teacher_overall, get_teacher_per_course

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/teacher-overall")
def teacher_overall(teacher_id: int = Query(..., description="Moodle teacher user id")):
    return get_teacher_overall(teacher_id)


@router.get("/teacher-per-course")
def teacher_per_course(
    teacher_id: int = Query(..., description="Moodle teacher user id"),
    course_id: int = Query(..., description="Moodle course id"),
):
    return get_teacher_per_course(teacher_id, course_id)
