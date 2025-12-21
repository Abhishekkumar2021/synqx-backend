from typing import List, Any
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app import models
from app.schemas import alert as alert_schema
from app.api import deps

router = APIRouter()

@router.post("/", response_model=alert_schema.AlertConfigRead, status_code=status.HTTP_201_CREATED)
def create_alert_config(
    *,
    db: Session = Depends(deps.get_db),
    alert_in: alert_schema.AlertConfigCreate,
    current_user: models.User = Depends(deps.get_current_user),
) -> Any:
    """
    Create a new alert configuration.
    """
    db_obj = models.AlertConfig(
        **alert_in.model_dump(),
        user_id=current_user.id,
        created_by=str(current_user.id)
    )
    db.add(db_obj)
    db.commit()
    db.refresh(db_obj)
    return db_obj

@router.get("/", response_model=List[alert_schema.AlertConfigRead])
def list_alert_configs(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_user),
    skip: int = 0,
    limit: int = 100,
) -> Any:
    """
    List alert configurations.
    """
    return db.query(models.AlertConfig).filter(
        models.AlertConfig.user_id == current_user.id
    ).offset(skip).limit(limit).all()

@router.delete("/{alert_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_alert_config(
    *,
    db: Session = Depends(deps.get_db),
    alert_id: int,
    current_user: models.User = Depends(deps.get_current_user),
) -> None:
    """
    Delete alert configuration.
    """
    alert = db.query(models.AlertConfig).filter(
        models.AlertConfig.id == alert_id,
        models.AlertConfig.user_id == current_user.id
    ).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert config not found")
    
    db.delete(alert)
    db.commit()
    return None

# Alert Instance Endpoints

@router.get("/history", response_model=List[alert_schema.AlertRead])
def list_alerts(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_user),
    skip: int = 0,
    limit: int = 100,
) -> Any:
    """
    List individual alerts.
    """
    # Alerts are linked to alert_configs which are linked to users
    return db.query(models.Alert).join(
        models.AlertConfig, models.Alert.alert_config_id == models.AlertConfig.id
    ).filter(
        models.AlertConfig.user_id == current_user.id
    ).order_by(models.Alert.created_at.desc()).offset(skip).limit(limit).all()

@router.patch("/history/{alert_id}", response_model=alert_schema.AlertRead)
def update_alert_status(
    *,
    db: Session = Depends(deps.get_db),
    alert_id: int,
    alert_in: alert_schema.AlertUpdate,
    current_user: models.User = Depends(deps.get_current_user),
) -> Any:
    """
    Update alert status (e.g., acknowledge).
    """
    alert = db.query(models.Alert).join(
        models.AlertConfig, models.Alert.alert_config_id == models.AlertConfig.id
    ).filter(
        models.Alert.id == alert_id,
        models.AlertConfig.user_id == current_user.id
    ).first()
    
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
    
    update_data = alert_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(alert, field, value)
    
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert

@router.get("/{alert_id}", response_model=alert_schema.AlertConfigRead)
def get_alert_config(
    *,
    db: Session = Depends(deps.get_db),
    alert_id: int,
    current_user: models.User = Depends(deps.get_current_user),
) -> Any:
    """
    Get alert configuration by ID.
    """
    alert = db.query(models.AlertConfig).filter(
        models.AlertConfig.id == alert_id,
        models.AlertConfig.user_id == current_user.id
    ).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert config not found")
    return alert

@router.patch("/{alert_id}", response_model=alert_schema.AlertConfigRead)
def update_alert_config(
    *,
    db: Session = Depends(deps.get_db),
    alert_id: int,
    alert_in: alert_schema.AlertConfigUpdate,
    current_user: models.User = Depends(deps.get_current_user),
) -> Any:
    """
    Update alert configuration.
    """
    alert = db.query(models.AlertConfig).filter(
        models.AlertConfig.id == alert_id,
        models.AlertConfig.user_id == current_user.id
    ).first()
    if not alert:
        raise HTTPException(status_code=404, detail="Alert config not found")
    
    update_data = alert_in.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(alert, field, value)
    
    db.add(alert)
    db.commit()
    db.refresh(alert)
    return alert
