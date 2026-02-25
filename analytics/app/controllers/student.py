from fastapi import APIRouter, Query
from ..services.student_service import get_student_overall, get_student_per_course

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/student-overall")
def student_overall(moodle_user_id: int = Query(..., description="Moodle user id")):
    return get_student_overall(moodle_user_id)


@router.get("/student-per-course")
def student_per_course(
    moodle_user_id: int = Query(..., description="Moodle user id"),
    course_id: int = Query(..., description="Moodle course id"),
):
    return get_student_per_course(moodle_user_id, course_id)
