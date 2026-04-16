FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
RUN mkdir -p bot-data
ENV PYTHONUNBUFFERED=1
EXPOSE 3000
CMD ["python", "bot.py"]
