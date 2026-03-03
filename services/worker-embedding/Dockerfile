FROM vllm/vllm-openai:latest

WORKDIR /app

COPY services/worker-embedding/entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/entrypoint.sh"]
