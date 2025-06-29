while IFS='=' read -r key value; do export "${key?}"="${value?}"; done < .env

sudo docker run -d \
    --name postgres \
    -e POSTGRES_USER=${POSTGRES_USER?} \
    -e POSTGRES_PASSWORD=${POSTGRES_PASSWORD?} \
    -e POSTGRES_DB=${POSTGRES_DATABASE?} \
    -p ${POSTGRES_PORT?}:5432 \
    -v ${PWD?}/database:/var/lib/postgresql/data \
    postgres:17