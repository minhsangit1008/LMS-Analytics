from fastapi import APIRouter
from ..services.admin_service import (
    get_admin_overall,
    get_admin_learning,
    get_admin_engagement,
    get_admin_ideas,
)

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/admin-overall")
def admin_overall():
    return get_admin_overall()


@router.get("/admin-learning")
def admin_learning():
    return get_admin_learning()


@router.get("/admin-engagement")
def admin_engagement():
    return get_admin_engagement()


@router.get("/admin-ideas")
def admin_ideas():
    return get_admin_ideas()
