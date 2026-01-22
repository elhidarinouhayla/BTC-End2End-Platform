from pydantic import BaseModel, Field
from datetime import datetime
from typing import Optional


class PredictionResponse(BaseModel):
    timestamp: datetime
    current_price: float
    predicted_price: float
    prediction_for: datetime
    
   


class PredictionRequest(BaseModel):
    open_price: float 
    high_price: float 
    low_price: float 
    close_price: float 
    volume: float 
    quote_asset_volume: float 
    number_of_trades: int 
    taker_buy_base_volume: float 
    taker_buy_quote_volume: float
     
    