FROM python:3.12.2-bookworm

WORKDIR /app

COPY requirements.txt .

RUN apt-get update && apt-get install -yq \
      gdal-bin \
      python3-gdal \
      libgdal-dev

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
