# Biblioteca musical NAS para Radio

Servicio Node.js independiente con catálogo, reproducción, programaciones y
panel web. No necesita dependencias de npm y guarda el catálogo y el audio en
`DATA_DIR` para que sobrevivan a los reinicios.

## Inicio rápido

1. Copia `.env.example` a `.env` y configura `API_KEY` con al menos 32 bytes
   aleatorios. Por ejemplo:

   ```sh
   node -e "console.log(require('node:crypto').randomBytes(32).toString('hex'))"
   ```

2. Construye y levanta el contenedor:

   ```sh
   docker compose up -d --build
   ```

3. Publica el puerto 3001 detrás del proxy HTTPS del NAS y abre
   `https://<nas>/admin`. Inicia sesión con la misma clave API configurada en
   Radio. En Radio, establece la URL base, sin `/api`, y la clave en
   **Administración → Radio**.

Para desarrollo local, con Node.js 20 o posterior:

```sh
npm test
npm start
```

`API_KEY`, `DATA_DIR`, `HOST` y `PORT` son configurables. El servidor escucha
por defecto en `0.0.0.0:3001`. Sin una clave válida de al menos 32 bytes, las
operaciones protegidas y el inicio de sesión del panel no se habilitan.

## Persistencia y seguridad

Docker Compose monta un volumen persistente en `/data`. Para un montaje NAS
directo, sustituye `biblioteca_data:/data` por una ruta persistente del NAS.
No publiques el servicio directamente en Internet: usa HTTPS en un proxy
inverso y limita también los intentos de autenticación en ese proxy. El
servicio limita a diez intentos fallidos por IP y minuto.

El panel valida la clave en el servidor, no la expone a JavaScript y usa una
cookie de sesión `HttpOnly` y `SameSite=Strict`. El proxy debe pasar
`X-Forwarded-Proto: https` para que la cookie también lleve `Secure`.

El catálogo y estado se guardan en `library.json`; los temas están en
`audio/<id-disco>/`. La carga y el reemplazo verifican el SHA-256 opcional y
reemplazan archivos mediante un temporal en el mismo volumen. Se aceptan
archivos de hasta 100 MiB y las extensiones AAC, M4A, MP3, OGG, OPUS y WAV.
Se admiten hasta 500 discos, 5000 temas y 200 programaciones.

Las rutas de API y formatos implementados corresponden al contrato entregado
para Radio. El panel permite crear y borrar discos, cargar varios temas,
reemplazar un archivo manteniendo su ID, editar la cola, controlar el
reproductor y gestionar programaciones. La programación diaria/semanal avanza
en intervalos de 24 horas/7 días desde la hora guardada.

El formulario de carga requiere seleccionar un disco existente, permite
preescuchar el audio y cambiar el nombre base manteniendo fija la extensión.
La carga muestra el porcentaje de progreso; el explorador inferior permite
buscar y filtrar temas por disco, preescucharlos y borrarlos.

## Despliegue en EasyPanel desde GitHub

1. Crea un servicio **App**, conecta `jsantdev/nas-radio` y selecciona la rama
   `main`. El **Build Path** debe ser `/`.
2. Selecciona **Nixpacks** como builder. El archivo `nixpacks.toml` del
   repositorio fija el comando de inicio (`npm start`), la escucha en
   `0.0.0.0:3001` y el directorio de datos `/app/data`.
3. En **Environment**, configura `API_KEY` con el mismo secreto de Radio,
   generado con al menos 32 bytes aleatorios. No lo pongas en archivos del
   repositorio.
4. En **Storage**, crea un volumen persistente con destino `/app/data`, y
   despliega de nuevo. Así la biblioteca no se pierde al recrear el servicio.
5. Configura el dominio HTTPS de EasyPanel con **Target Port `3001`** y
   despliega el servicio. El proceso escucha en el puerto interno `3001`; el
   puerto público puede ser otro porque el proxy enruta al puerto interno.

Si modificas variables, almacenamiento o dominio, guarda esos cambios y vuelve
a seleccionar **Deploy** en EasyPanel. En deployments anteriores, verifica que
el servicio use la revisión más reciente de `main`, que el Build Path sea `/`,
y que el destino del dominio siga apuntando a `3001`.
