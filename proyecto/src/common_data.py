from dataclasses import dataclass
from pathlib import Path

@dataclass(frozen=True)
class DIR:
    PROJECT_ROOT: Path = Path(__file__).parent.parent  # proyecto
    DATA: Path = Path(PROJECT_ROOT / 'data/')
    DATA_CATOGRAFIA: Path = Path(DATA / 'cartografia-secciones/')
    GRAPHS: Path = Path(PROJECT_ROOT / 'graficos/')

@dataclass(frozen=True)
class FORMAT:
    CSV: str = 'csv'
    JSON: str = 'json'
    GEOJSON = "geojson"

@dataclass(frozen=True)
class DATASET:
    ACTIVIDAD_SC: str = 'actividad-sc-3.csv'
    DISTRIBUCIÓN_RENTA_INGRESOS: str = 'distribucion-renta-ingresos.csv'
    OCUPACION_SC: str = 'ocupacion-sc-3.csv'
    RENTA_MEDIA_SC: str = 'rentamedia-sc-3.csv'
    CARTOGRAFIA_TENERIFE_2021: str = 'secciones_20210101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2022: str = 'secciones_20220101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2023: str = 'secciones_20230101_tenerife.json'
    CARTOGRAFIA_TENERIFE_2024: str = 'secciones_20240101_tenerife.json'

MUNICIPIO_ISLA = {
    # =======================
    # TENERIFE
    # =======================
    'Adeje': 'Tenerife',
    'Arafo': 'Tenerife',
    'Arico': 'Tenerife',
    'Arona': 'Tenerife',
    'Buenavista del Norte': 'Tenerife',
    'Candelaria': 'Tenerife',
    'El Rosario': 'Tenerife',
    'El Sauzal': 'Tenerife',
    'El Tanque': 'Tenerife',
    'Fasnia': 'Tenerife',
    'Garachico': 'Tenerife',
    'Granadilla de Abona': 'Tenerife',
    'Guía de Isora': 'Tenerife',
    'Güímar': 'Tenerife',
    'Icod de los Vinos': 'Tenerife',
    'La Guancha': 'Tenerife',
    'La Matanza de Acentejo': 'Tenerife',
    'La Orotava': 'Tenerife',
    'Los Realejos': 'Tenerife',
    'Los Silos': 'Tenerife',
    'Puerto de La Cruz': 'Tenerife',
    'Puerto de la Cruz': 'Tenerife',
    'San Cristóbal de La Laguna': 'Tenerife',
    'San Juan de la Rambla': 'Tenerife',
    'San Miguel de Abona': 'Tenerife',
    'Santa Cruz de Tenerife': 'Tenerife',
    'Santa Úrsula': 'Tenerife',
    'Santiago del Teide': 'Tenerife',
    'Tacoronte': 'Tenerife',
    'Tegueste': 'Tenerife',
    'Vilaflor de Chasna': 'Tenerife',
    'El Rosario': 'Tenerife',
    'El Sauzal': 'Tenerife',
    'La Victoria de Acentejo': 'Tenerife',

    # =======================
    # LA PALMA
    # =======================
    'Barlovento': 'La Palma',
    'Breña Alta': 'La Palma',
    'Breña Baja': 'La Palma',
    'Fuencaliente de La Palma': 'La Palma',
    'Fuencaliente de la Palma': 'La Palma',
    'Garafía': 'La Palma',
    'Los Llanos de Aridane': 'La Palma',
    'El Paso': 'La Palma',
    'Puntagorda': 'La Palma',
    'Puntallana': 'La Palma',
    'San Andrés y Sauces': 'La Palma',
    'Santa Cruz de La Palma': 'La Palma',
    'Santa Cruz de la Palma': 'La Palma', 
    'Tijarafe': 'La Palma',
    'Villa de Mazo': 'La Palma',
    'Tazacorte': 'La Palma',
    'Valleseco': 'Gran Canaria',

    # =======================
    # LA GOMERA
    # =======================
    'Agulo': 'La Gomera',
    'Alajeró': 'La Gomera',
    'Hermigua': 'La Gomera',
    'San Sebastián de La Gomera': 'La Gomera',
    'San Sebastián de la Gomera': 'La Gomera',
    'Valle Gran Rey': 'La Gomera',
    'Vallehermoso': 'La Gomera',

    # =======================
    # EL HIERRO
    # =======================
    'Frontera': 'El Hierro',
    'Valverde': 'El Hierro',
    'El Pinar de El Hierro': 'El Hierro',

    # =======================
    # GRAN CANARIA
    # =======================
    'Agaete': 'Gran Canaria',
    'Agüimes': 'Gran Canaria',
    'Artenara': 'Gran Canaria',
    'Arucas': 'Gran Canaria',
    'Firgas': 'Gran Canaria',
    'Gáldar': 'Gran Canaria',
    'Ingenio': 'Gran Canaria',
    'La Aldea de San Nicolás': 'Gran Canaria',
    'Las Palmas de Gran Canaria': 'Gran Canaria',
    'Mogán': 'Gran Canaria',
    'Moya': 'Gran Canaria',
    'San Bartolomé de Tirajana': 'Gran Canaria',
    'Santa Brígida': 'Gran Canaria',
    'Santa Lucía de Tirajana': 'Gran Canaria',
    'Santa María de Guía de Gran Canaria': 'Gran Canaria',
    'Tejeda': 'Gran Canaria',
    'Telde': 'Gran Canaria',
    'Teror': 'Gran Canaria',
    'Valsequillo de Gran Canaria': 'Gran Canaria',
    'Vega de San Mateo': 'Gran Canaria',

    # =======================
    # LANZAROTE
    # =======================
    'Arrecife': 'Lanzarote',
    'Haría': 'Lanzarote',
    'San Bartolomé': 'Lanzarote',
    'Teguise': 'Lanzarote',
    'Tías': 'Lanzarote',
    'Tinajo': 'Lanzarote',
    'Yaiza': 'Lanzarote',

    # =======================
    # FUERTEVENTURA
    # =======================
    'Antigua': 'Fuerteventura',
    'Betancuria': 'Fuerteventura',
    'La Oliva': 'Fuerteventura',
    'Pájara': 'Fuerteventura',
    'Puerto del Rosario': 'Fuerteventura',
    'Tuineje': 'Fuerteventura',
}