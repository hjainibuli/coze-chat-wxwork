# 使用官方轻量级 Python 镜像
FROM python:3.10-slim


# --- [新增] 环境变量优化 ---
# PYTHONDONTWRITEBYTECODE=1: 防止生成 .pyc 文件，减小体积
# PYTHONUNBUFFERED=1: 强制实时输出日志，防止 Docker logs 丢失或延迟
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# 设置工作目录
WORKDIR /app

# 设置时区为上海 (解决日志时间问题)
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' > /etc/timezone

# 1. 先复制依赖文件 (利用 Docker 缓存层加速构建)
COPY app/requirements.txt .

# 2. ffmpeg：WAV→MP3；build-essential：pilk 在 arm64 等常无 wheel 需 gcc 编译；pip 装完后卸掉编译链减小体积
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    build-essential \
    && pip install --no-cache-dir -r requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple \
    && apt-get purge -y build-essential \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/*

# 3. 复制所有业务代码
COPY app/ .

# 暴露端口 (仅供容器间通信)
EXPOSE 8000

# 启动命令 (使用 Gunicorn 生产模式)
# 注意：超时时间设为 120s 配合 Nginx
CMD ["gunicorn", "main:app", "-w", "5", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--timeout", "120", "--log-level", "info"]