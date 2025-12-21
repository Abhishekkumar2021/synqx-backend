from typing import List, Optional
from sqlalchemy.orm import Session
from datetime import datetime, timezone

from app import models
from app.models.enums import AlertType, AlertLevel, AlertStatus, AlertDeliveryMethod
from app.core.logging import get_logger

import json
import redis
from app.core.config import settings

logger = get_logger(__name__)

class AlertService:
    @staticmethod
    def trigger_alerts(
        db: Session,
        alert_type: AlertType,
        pipeline_id: int,
        job_id: Optional[int] = None,
        message: Optional[str] = None,
        level: AlertLevel = AlertLevel.INFO
    ) -> List[models.Alert]:
        """
        Trigger alerts based on configuration for a specific event.
        """
        try:
            pipeline = db.query(models.Pipeline).filter(models.Pipeline.id == pipeline_id).first()
            if not pipeline:
                logger.warning(f"Pipeline {pipeline_id} not found for alert trigger")
                return []

            # Find matching configurations
            configs = db.query(models.AlertConfig).filter(
                models.AlertConfig.enabled == True,
                models.AlertConfig.alert_type == alert_type,
                models.AlertConfig.user_id == pipeline.owner_id
            ).all()

            alerts = []
            redis_client = redis.from_url(settings.REDIS_URL, decode_responses=True)

            for config in configs:
                # Check cooldown
                if config.last_triggered_at:
                    cooldown_seconds = config.cooldown_minutes * 60
                    if (datetime.now(timezone.utc) - config.last_triggered_at.replace(tzinfo=timezone.utc)).total_seconds() < cooldown_seconds:
                        logger.info(f"Alert config {config.id} in cooldown")
                        continue

                alert_msg = message or f"Alert triggered for pipeline {pipeline.name}"
                
                alert = models.Alert(
                    alert_config_id=config.id,
                    pipeline_id=pipeline_id,
                    job_id=job_id,
                    message=alert_msg,
                    level=level,
                    status=AlertStatus.PENDING,
                    delivery_method=config.delivery_method,
                    recipient=config.recipient
                )
                db.add(alert)
                db.flush() # Get the ID
                
                config.last_triggered_at = datetime.now(timezone.utc)
                db.add(config)
                alerts.append(alert)

                # Broadcast to WebSocket
                try:
                    notification_payload = {
                        "id": alert.id,
                        "type": "new_alert",
                        "message": alert.message,
                        "level": alert.level.value,
                        "job_id": alert.job_id,
                        "pipeline_id": alert.pipeline_id,
                        "created_at": datetime.now(timezone.utc).isoformat()
                    }
                    redis_client.publish(f"user_notifications:{pipeline.owner_id}", json.dumps(notification_payload))
                except Exception as broadcast_err:
                    logger.error(f"Failed to broadcast notification: {broadcast_err}")

            db.commit()
            return alerts
        except Exception as e:
            logger.error(f"Error triggering alerts: {e}", exc_info=True)
            return []
