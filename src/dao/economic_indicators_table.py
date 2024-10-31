from sqlmodel import Field, Session, SQLModel, select
from typing import Optional

class IndicatorsTable(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    date: str
    indice_de_actividad_economica: Optional[float] = None
    encuesta_de_grupo_trabajador_ajustada_estacionalmente: Optional[float] = None
    encuesta_de_grupo_trabajador: Optional[float] = None
    encuesta_de_establecimientos_ajustados_estacionalmente: Optional[float] = None
    encuesta_de_establecimientos: Optional[float] = None
    indicadores_de_turismo: Optional[float] = None
    indicadores_de_construccion: Optional[float] = None
    indicadores_de_ingresos_netos: Optional[float] = None 
    indicadores_de_energia_electrica: Optional[float] = None
    indicadores_de_comercio_exterior: Optional[float] = None
    indicadores_de_quiebras: Optional[float] = None
    indicadores_de_ventas_al_detalle_a_precios_corrientes: Optional[float] = None
    precios_promedios_mensuales_de_gasolina_al_detal_en_puerto_rico: Optional[float] = None 
    indice_de_precios_al_consumidor_2006_100: Optional[float] = None
    indicadores_de_transportacion: Optional[float] = None
    indices_coincidentes_de_actividad_economica: Optional[float] = None
    encuesta_de_establecimientos_manufactura: Optional[float] = None

def create_indicators_table(engine):
    SQLModel.metadata.drop_all(engine)
    SQLModel.metadata.create_all(engine)
