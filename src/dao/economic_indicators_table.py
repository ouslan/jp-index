from sqlmodel import Field, Session, SQLModel, select
from typing import Optional

class IndicatorsTable(SQLModel, table=True):
    id: Optional[int] = Field(primary_key=True)
    date: str
    indice_de_actividad_economica: float
    encuesta_de_grupo_trabajador_ajustada_estacionalmente: float
    encuesta_de_grupo_trabajador: float
    encuesta_de_establecimientos_ajustados_estacionalmente: float
    encuesta_de_establecimientos: float
    indicadores_de_turismo: float
    indicadores_de_construccion: float
    indicadores_de_ingresos_netos: float
    indicadores_de_energia_electrica: float
    indicadores_de_comercio_exterior: float
    indicadores_de_quiebras: float
    indicadores_de_ventas_al_detalle_a_precios_corrientes: float
    indicadores_de_ventas_al_detalle_a_precios_corrientes: float
    precios_promedios_mensuales_de_gasolina_al_detal_en_puerto_rico: float
    indice_de_precios_al_consumidor_2006_100: float
    indicadores_de_transportacion: float
    indices_coincidentes_de_actividad_economica: float
    encuesta_de_establecimientos_manufactura: float

def create_indicators_table(engine):
    SQLModel.metadata.create_all(engine)

def select_all_indicators(engine):
    with Session(engine) as session:
        statement = select(IndicatorsTable)
        return session.exec(statement).all()


