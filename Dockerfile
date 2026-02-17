# ==========================================
# STAGE 1: Constructor (builder)
# ==========================================
FROM apache/airflow:3.1.7 AS builder

# 1. Cambiamos a root para instalar herramientas de compilación necesarias para algunas librerías (como Polars)
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         build-essential \
         libpq-dev \
         git \
         python3-venv \  
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 2. Volvemos al usuario airflow para instalar paquetes Python
USER airflow
# Copiamos ambos archivos
COPY requirements.txt /requirements.txt
COPY requirements-dbt.txt /requirements-dbt.txt


# ==========================================
# STAGE 1A: Entorno Virtual para DBT
# ==========================================
# Instalamos las librerías en el directorio de usuario (--user)
# Instala todo en /home/airflow/.local
# Definimos las versiones exactas de Airflow y Python para asegurar compatibilidad con las librerías
ARG AIRFLOW_VERSION=3.1.7
ARG PYTHON_VERSION=3.12
ARG CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Usamos constraints para asegurar que Airflow no se rompa por incompatibilidades de versiones
RUN pip install --user --no-cache-dir \
    -r /requirements.txt \
    -c ${CONSTRAINT_URL}


# ==========================================
# STAGE 1B: Entorno Virtual para DBT
# ==========================================
# Creamos un entorno virtual dedicado para dbt
RUN python3 -m venv /home/airflow/.dbt_venv

# Instalamos dbt dentro sin constraints
RUN /home/airflow/.dbt_venv/bin/pip install --no-cache-dir \
    -r /requirements-dbt.txt


# ==========================================
# STAGE 2: Imagen Final (Produccion)
# ==========================================
FROM apache/airflow:3.1.7

USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         libpq5 \
         git \
         locales \
&& sed -i -e 's/# en_US.UTF-8 UTF-8/en_US.UTF-8 UTF-8/' /etc/locale.gen \
  && locale-gen \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

  # Definimos las variables de entorno para que persistan en el contenedor final
ENV LANG=en_US.UTF-8 \
    LANGUAGE=en_US:en \
    LC_ALL=en_US.UTF-8

USER airflow

# 1. Copiamos las librerías principales (Polars, etc.)
COPY --from=builder --chown=airflow:0 /home/airflow/.local /home/airflow/.local

# 2. Copiamos el entorno virtual de DBT
COPY --from=builder --chown=airflow:0 /home/airflow/.dbt_venv /home/airflow/.dbt_venv

# 3. Enlace simbólico (Symlink)
# Esto hace que cuando se escriba 'dbt' en la terminal, Linux ejecute el del entorno virtual
# pero sin afectar las librerías de Airflow.
USER root
RUN ln -s /home/airflow/.dbt_venv/bin/dbt /usr/local/bin/dbt
USER airflow

ENV PATH=/home/airflow/.local/bin:$PATH