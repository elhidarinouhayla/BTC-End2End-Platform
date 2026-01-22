from .base import Base
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Float
from datetime import datetime
from sqlalchemy.orm import relationship


class Prediction(Base):
    __tablename__ = "predictions"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    
    timestamp = Column(DateTime, nullable=False, index=True)
    prediction_for = Column(DateTime, nullable=False)
    
    current_price = Column(Float, nullable=False)
    predicted_price = Column(Float, nullable=False)
    actual_price = Column(Float, nullable=True)
    
    confidence_lower = Column(Float, nullable=True)
    confidence_upper = Column(Float, nullable=True)
    
    model_version = Column(String(50), nullable=False, default="v1.0.0")
    error = Column(Float, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    user = relationship("User", back_populates="predictions")