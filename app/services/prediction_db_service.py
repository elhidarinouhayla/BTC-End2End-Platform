from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from fastapi import HTTPException
from app.db.models.prediction import Prediction


def save_prediction(
    db: Session,
    user_id: str,
    current_time: datetime,
    current_price: float,
    predicted_price: float,
    confidence_lower: float = None,
    confidence_upper: float = None,
    model_version: str = "v1.0.0"
) -> Prediction:
    """Sauvegarde une prédiction en base de données.
    
    actual_price et error seront mis à jour 10 minutes plus tard par la tâche planifiée.
    """
    try:
        new_prediction = Prediction(
            user_id=user_id,
            timestamp=current_time,
            prediction_for=current_time + timedelta(minutes=10),  # T+10 minutes
            current_price=current_price,
            predicted_price=predicted_price,
            confidence_lower=confidence_lower,
            confidence_upper=confidence_upper,
            actual_price=None,  # Sera mis à jour à T+10
            error=None,  # Sera calculé à T+10
            model_version=model_version
        )
        
        db.add(new_prediction)
        db.commit()
        db.refresh(new_prediction)
        
        print(f" Prédiction sauvegardée avec ID: {new_prediction.id}")
        print(f"⏰ Sera validée à: {new_prediction.prediction_for}")
        return new_prediction
        
    except Exception as db_error:
        db.rollback()
        print(f" Erreur DB: {db_error}")
        raise HTTPException(500, f"Erreur de sauvegarde: {str(db_error)}")


def update_prediction_actual(
    db: Session,
    prediction_id: int,
    actual_price: float
) -> Prediction:
    """Met à jour le prix réel à T+10 et calcule l'erreur."""
    try:
        prediction = db.query(Prediction).filter(Prediction.id == prediction_id).first()
        
        if not prediction:
            raise HTTPException(404, f"Prédiction {prediction_id} non trouvée")
        
        # Mettre à jour le prix réel à T+10
        prediction.actual_price = actual_price
        
        # Calculer l'erreur absolue (MAE)
        prediction.error = abs(actual_price - prediction.predicted_price)
        
        db.commit()
        db.refresh(prediction)
        
        print(f" Prédiction {prediction_id} mise à jour")
        print(f"   Prédit: {prediction.predicted_price:.2f} | Réel: {actual_price:.2f} | Erreur: {prediction.error:.2f}")
        
        return prediction
        
    except HTTPException:
        raise
    except Exception as db_error:
        db.rollback()
        print(f" Erreur DB: {db_error}")
        raise HTTPException(500, f"Erreur de mise à jour: {str(db_error)}")


def get_predictions_to_update(db: Session, limit: int = 100) -> list[Prediction]:
    """Récupère les prédictions dont T+10 est passé et qui n'ont pas de prix réel."""
    try:
        current_time = datetime.utcnow()
        
        predictions = db.query(Prediction).filter(
            Prediction.prediction_for <= current_time,  # T+10 est passé
            Prediction.actual_price.is_(None)  # Pas encore mis à jour
        ).order_by(Prediction.prediction_for).limit(limit).all()
        
        print(f" {len(predictions)} prédictions à mettre à jour (T+10 écoulé)")
        return predictions
        
    except Exception as db_error:
        print(f" Erreur DB: {db_error}")
        return []