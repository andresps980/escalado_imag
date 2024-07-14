import boto3
import concurrent.futures
import json
import datetime
import requests
import io
import os
import string
import random
import colorthief
from colorthief import ColorThief
import base64
import itertools

from botocore.exceptions import ClientError

from utils.gestion_imagenes import load_image_from_url, \
    calcula_dimensiones_reescalado, \
    is_predominantly_white, \
    identify_filetype, \
    procedimiento_de_reescalado_imagen_por_ai, \
    make_gif, \
    get_total_frames

from utils.qr_utils import make_qr, \
    adjust_qr_to_target_size

from PIL import Image as Image_pil

# TODO Andres
# https://pywombat.com/articles/ipython-comandos-magicos
# from IPython.display import Image as Image_Ipython
# from IPython.display import Image, display
# from IPython.display import Image as DisplayImage


# Creamos una sesion de AWS accedemos API de AWS

AWS_REGION = 'eu-west-1'
MYKEY = 'XXxxxxxxxxx'
MYSECRET = 'xxxxxx'

# CREACION PARA DYNAMO_DB
TABLE_NAME = 'bitv_ads_transform'

# Buckets de S3 a emplear
BUCKET_QRS = 'bitv-qrs'
BUCKET_ADS = 'bitv-ads'

# Cola de recepcion de peticiones de escalado
QUEUE_NAME = 'bitv_ad_transform'


def create_session(logger):
    session_aws = boto3.Session(
        aws_access_key_id=MYKEY,
        aws_secret_access_key=MYSECRET,
        region_name=AWS_REGION)

    # Initialize the SQS client using the existing session
    sqs = session_aws.client('sqs')

    # Creamos una cola SQS por primera vez sobre AWS (es la cola de anuncios a las que las TVs envían en anuncio si
    # es la primera vez que lo ven, para ser reescalados "off line" y en no tiempo real. Try to get the URL of the
    # queue
    queue_url = None
    try:
        response = sqs.get_queue_url(QueueName=QUEUE_NAME)
        logger.info(f"The queue '{QUEUE_NAME}' exists and its URL is: {response['QueueUrl']}")
        queue_url = response['QueueUrl']

    except sqs.exceptions.QueueDoesNotExist:
        logger.info(f"The queue '{QUEUE_NAME}' does not exist.")

        # Create an SQS queue using the existing session
        sqs = session_aws.resource('sqs')
        queue = sqs.create_queue(QueueName=QUEUE_NAME)
        queue_url = queue.url

        logger.info(f"Created a new queue '{QUEUE_NAME}' with URL: {queue_url}")

    except ClientError as e:
        logger.error("Exception ClientError aws, creando acceso a colas", exc_info=True)

    except Exception as e:
        logger.error("Exception general, creando acceso a colas", exc_info=True)

    return session_aws, sqs, queue_url


def get_message_body(message):
    message_body = json.loads(message['Body'])
    sent_timestamp = int(datetime.datetime.fromtimestamp(
        int(message['Attributes']['SentTimestamp']) / 1000
    ).timestamp())
    # TODO porque queremos esto?
    sent_date_time = datetime.datetime.fromtimestamp(int(message['Attributes']['SentTimestamp']) / 1000)
    message_body['SentTimestamp'] = sent_timestamp

    return message_body


def send_message_sqs(sqs_, url_ad_, url_click_, queue_url_):
    message = {
        'url_ad': url_ad_,
        'url_click': url_click_,
    }

    # Enviamos mensaje a la cola
    response = sqs_.send_message(
        QueueUrl=queue_url_,
        MessageBody=json.dumps(message)
    )

    return response


def get_messages_from_sqs_parallel(sqs, queue_url, num_messages, logger):
    messages = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_message = {executor.submit(sqs.receive_message, QueueUrl=queue_url, MaxNumberOfMessages=1,
                                             AttributeNames=['SentTimestamp']): i for i in range(num_messages)}
        for future in concurrent.futures.as_completed(future_to_message):
            try:
                response = future.result()
                if 'Messages' in response:
                    message = response['Messages'][0]
                    messages.append(get_message_body(message))

                    # Opción de borrar los mensajes de la cola una vez recogidos
                    sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=message['ReceiptHandle'])

            except Exception as e:
                logger.error("Exception general, tratando mensajes en cola", exc_info=True)
    return messages


def crear_tabla_dinamodb(session_aws):
    # Define the schema of the table
    dydb = session_aws.client('dynamodb')
    key_schema = [
        {
            'AttributeName': 'nombre_imagen',
            'KeyType': 'HASH'
        }
    ]

    attribute_definitions = [
        {
            'AttributeName': 'nombre_imagen',
            'AttributeType': 'S'
        }
    ]
    # Create a new DynamoDB table
    response = dydb.create_table(
        TableName=TABLE_NAME,
        KeySchema=key_schema,
        AttributeDefinitions=attribute_definitions,
        ProvisionedThroughput={
            'ReadCapacityUnits': 10,
            'WriteCapacityUnits': 10
        },
    )

    # Wait for the table to be created
    waiter = dydb.get_waiter('table_exists')
    waiter.wait(TableName=table_name)

    # Print the table description
    table_description = dydb.describe_table(TableName=table_name)
    # print(f"Table '{table_name}' created successfully!")
    # print("Table description:")
    # print(table_description)
    # Instantiate the DynamoDB table object
    return table_description


def process_images(paths, logger, session_aws):
    # TODO Cual era el objetivo?
    # DEfinimos un dict de los resultados
    dict_resultados = {}

    # List of keys
    keyList = ["nombre_imagen", "timestamp_creacion", "s3_imagen", 's3_qr', 'url_imagen', 'url_click',
               'url_click_short']

    # iterating through the elements of list
    for i in keyList:
        dict_resultados[i] = None

    table = dame_tabla_dinamodb(logger, session_aws)
    if table is None:
        logger.error(f'Salimos del proceso, {len(paths)} imagenes no seran procesadas')
        return

    s3 = session_aws.client('s3')

    for path in paths:

        logger.info('--------------- Comienzo procesamiento ---------------')

        # TODO Andres: Para pruebas de imagen voy a guardar de momento en un dir por escalado
        characters = string.ascii_letters
        result_str = ''.join(random.choice(characters) for _ in range(5))
        path_base = '.'
        temp_folder = os.path.join(path_base, "img", 'imagenes_temp_' + result_str)
        os.mkdir(temp_folder)

        # Tenemos una entrada de un json con la URL de la imagen y la url de click.
        # Tenemos que obtener URL de ambas.
        # path es un DICT
        url_imagen = path['url_ad']
        url_click = path['url_click']
        timestamp_creacion = path['SentTimestamp']

        # Descargamos la imagen desde el CDN del anunciante y calculamos algunos datos de ella
        tipo_imagen, imagen, filenames_list, durations_list = load_image_from_url(url_imagen, temp_folder, logger)

        if imagen is not None:
            try:
                # Obtenemos el nombre de fichero de imagen y qr
                nombre_fichero_imagen, nombre_fichero_base64 = obtiene_nombre_fichero(url_imagen, logger)
                # TODO Andres, mejor dejar la terminacion como el tipo?
                nombre_fichero_imagen_a_guardar = nombre_fichero_base64 + '.' + tipo_imagen
                nombre_fichero_qr_a_guardar = nombre_fichero_base64 + '.qr'

                # Hacemos un preprocesado de los QRs:  las dimensiones y color adecuado a la imagen Obtenemos el
                # valor de la nuevas dimensiones reescaladas para formato TV de la primera imagen de la lista También
                # obtenemos el color dominante para los pixeles del QR pero que resalten sobre fondo blanco
                dim, dominant_color, target_size = extraer_info_imagen(filenames_list, imagen, logger, tipo_imagen)
                logger.info(f'Tamaño final destino 2: {target_size}')
                logger.info(f'Color dominante para el tipo {tipo_imagen}: {dominant_color}')

                # TODO Terminar de decidir como va esto... si todo va a ser modelo 5...
                # Ahora tenemos que elegir el modelos de ampliación a aplicar y su calidad
                # La estrategia es las imágenes prequeñas 300x600 p. ej multiplar por x4 y más calidad
                # y reducir o ampliar el ajuste final por CV2 hasta el resultado necesario
                # El resto multiplicar por x2 y reducir por CV2 hasta tamaño necesario
                # Elección de modelo de superesolución Para imágenes pequeñas usamos más calidad y x4 (más lento)
                if dim == (300, 100) or dim == (300, 250):
                    modelo = 4  # modelo x4 y mejor calidad al ser un formato pequeño de partida
                else:
                    modelo = 5  # modelo x2 y reduccion posterior
                modelo = 5

                # TODO Terminar de definir esta parte, de momento el mismo url_short para todos
                # Calculamos y guardamos el valor de la url_corta con la api de nuestro proveedor externo
                domain = '9h5q.short.gy'
                url_click_short = obtiene_url_short(domain, url_click)

                # Creamos un qr y obtenemos el nombre del fichero donde se ha creado
                nombre_fichero_qr_temp = make_qr(temp_folder, url_click_short, dominant_color, light_='white')
                qr_image = Image_pil.open(nombre_fichero_qr_temp)

                # Ajustamos el QR al tamaño determinado para este tipo
                if target_size > 300:
                    target_size = 300

                # Generamos un QR reescalado y obtnemos el valor del fichero donde se ha depositado
                nombre_fichero_qr_temp_reescalado = adjust_qr_to_target_size(nombre_fichero_qr_temp,
                                                                             target_size,
                                                                             temp_folder,
                                                                             logger)

                logger.info(f'Nombre fichero imagen img: {nombre_fichero_imagen_a_guardar}')
                logger.info(f'Nombre fichero imagen qr: {nombre_fichero_qr_a_guardar}')
                logger.info(f'Tipo de imagen obtenida: {tipo_imagen}')

                # si el tipo de imagen es GIF animado, tenemos que ver cuantas images tiene
                # Recorvertir una a uno cada frame a tipo admisible por CV2
                # reescarlarlas
                # Volver a montar el GIF animado con los PNGs reescalados
                if len(nombre_fichero_base64) > 100:
                    nombre_normalizado = ''.join(itertools.islice(nombre_fichero_base64, 100))
                else:
                    nombre_normalizado = nombre_fichero_base64

                if tipo_imagen == 'gif':
                    resized_gif_frames_files = []

                    for file in filenames_list:
                        imagen_aux = Image_pil.open(file)
                        static_image = imagen_aux.convert('RGB')
                        resized_image = procedimiento_de_reescalado_imagen_por_ai(modelo, static_image)
                        resized_image_file = io.BytesIO()
                        resized_image.save(resized_image_file, format='PNG')
                        resized_image_file.seek(0)
                        resized_gif_frames_files.append(resized_image_file)

                    logger.info(f'Total de resized_gif_frames_files: {len(resized_gif_frames_files)}')

                    nombre_fichero_a_guardar = os.path.join(temp_folder, "FINAL_" + nombre_normalizado + '.gif')
                    logger.info(f'Nombre fichero a guardar: {nombre_fichero_a_guardar}')

                    make_gif(resized_gif_frames_files, durations_list, nombre_fichero_a_guardar, 1)
                    logger.info(f'Numero frames imagen final gif: {get_total_frames(nombre_fichero_a_guardar)}')

                elif tipo_imagen == 'png':
                    resized_image = procedimiento_de_reescalado_imagen_por_ai(modelo, imagen)
                    # Guardamos en disco
                    with resized_image as im:
                        # Guardamnos el png
                        nombre_fichero_a_guardar = os.path.join(temp_folder, "FINAL_" + nombre_normalizado + '.png')
                        im.save(nombre_fichero_a_guardar, format='PNG')
                    logger.info(f'Nombre fichero a guardar: {nombre_fichero_a_guardar}')

                elif tipo_imagen == 'jpeg':
                    resized_image = procedimiento_de_reescalado_imagen_por_ai(modelo, imagen)
                    # Guardamos en disco
                    with resized_image as im:
                        nombre_fichero_a_guardar = os.path.join(temp_folder, "FINAL_" + nombre_normalizado + '.jpeg')
                        # Guardamnos el png
                        im.save(nombre_fichero_a_guardar, format='JPEG')
                    logger.info(f'Nombre fichero a guardar: {nombre_fichero_a_guardar}')

                bucket_name_imagenes = BUCKET_ADS
                bucket_name_qrs = BUCKET_QRS
                bucket_s3_imagenes = "s3://" + bucket_name_imagenes
                bucket_s3_qrs = "s3://" + bucket_name_qrs
                nombre_fichero_imagenes_a_guardar_s3 = bucket_s3_imagenes + "/" + nombre_fichero_imagen_a_guardar
                nombre_fichero_qr_a_guardar_s3 = bucket_s3_qrs + "/" + nombre_fichero_qr_a_guardar
                logger.info(f'Nombre fichero imagen img en s3: {nombre_fichero_imagenes_a_guardar_s3}')
                logger.info(f'Nombre fichero imagen qr en s3: {nombre_fichero_qr_a_guardar_s3}')
                # Ya tenemos la imagen redimensionada tanto si es un GIF como no
                # También tenemos la imagen del QR generado

                # TODO Investigar la idea de usar solo imgen en memoria
                # TODO Hacemos algo con la respuesta?
                # write resized image + qr image  to S3 buckets
                # s3.put_object(Bucket=bucket_name_imagenes, Key=nombre_fichero_imagen + '.img',
                #               Body=resized_image.tobytes())
                response = s3.upload_file(nombre_fichero_a_guardar,
                                          bucket_name_imagenes,
                                          nombre_fichero_imagen_a_guardar)
                logger.info(f'Respuesta s3 al guardar imagen: {response}')
                # s3.put_object(Bucket=bucket_name_qrs, Key=nombre_fichero_imagen + '.qr', Body=qr_image.tobytes())
                response = s3.upload_file(nombre_fichero_qr_temp_reescalado,
                                          bucket_name_qrs,
                                          nombre_fichero_qr_a_guardar)
                logger.info(f'Respuesta s3 al guardar qr: {response}')

                # Subo los resultados a Dynamo
                item = {
                    'nombre_imagen': str(nombre_fichero_imagen),
                    'SentTimestamp': timestamp_creacion,
                    'url_ad': url_imagen,
                    'url_click': url_click,
                    's3_url_imagen': nombre_fichero_imagenes_a_guardar_s3,
                    's3_url_qr': nombre_fichero_qr_a_guardar_s3,
                    'url_click_short': 'TESTTTT',
                }

                bulk_load_items(item, table)

                # TODO Andres de momento no borramos para ver resultados
                # # Quitamos los fichero empleados.
                # # Specify the directory path where the files are located
                # directory_path = '.\\'
                #
                # # Specify the string to search for in filenames
                # search_strings = ['qr_temp', 'png', 'gif_frame', 'imgen_temp']
                #
                # # Call the function to remove files containing the specified string in the directory
                # for string_aux in search_strings:
                #     remove_files_with_string(directory_path, string_aux)
                #
                # os.remove(nombre_fichero_a_guardar)
                # os.remove(nombre_fichero_qr_a_guardar)
                # # os.remove(filenames_list)
                # # os.remove(nombre_fichero_qr_temp)
                # # os.remove(nombre_fichero_qr_temp_reescalado)
                # # os.remove('temp_resized_gif.gif')

            except Exception as e:
                logger.error(f"Exception escalando la imagen con URL: {url_imagen}, mensaje: ", exc_info=True)

        else:  # si la imagen que se ha subido desde la URL resulta vacía.
            logger.info(f"WARNING Imagen vacia al intentar cargarla, URL: {url_imagen}")

        # Fin de análisi de todas las imágenes de un BATCH

    return  # de toda la función de reescalado , generacion de QRs y de subida a almancenamiento S3


def dame_tabla_dinamodb(logger, session_aws):
    # Abrimos session para dinamo y creamos tabla si no existe
    dynamodb = boto3.resource(
        'dynamodb',
        aws_access_key_id=MYKEY,
        aws_secret_access_key=MYSECRET,
        region_name=AWS_REGION
    )
    table = None
    try:
        table = dynamodb.Table(TABLE_NAME)
    except dynamodb.exceptions.ResourceNotFoundException:
        # TODO Probar la creacion de la tabla...
        table_description = crear_tabla_dinamodb(session_aws)
        table = dynamodb.Table(TABLE_NAME)
        logger.warning(f'Tabla {TABLE_NAME} no existe y sera creada.', exc_info=True)
    except Exception as e:
        logger.error(f"Exception general creando la tabla: {TABLE_NAME}, mensaje: ", exc_info=True)
        table = None
    return table


def truncateTable(table):
    # get the table keys
    table_key_names = [key.get("AttributeName") for key in table.key_schema]

    """
    NOTE: there are reserved attributes for key names, please see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/ReservedWords.html
    if a hash or range key is in the reserved word list, you will need to use the ExpressionAttributeNames parameter
    described at https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Table.scan
    """

    # Only retrieve the keys for each item in the table (minimize data transfer)
    projection_expression = ", ".join(table_key_names)

    response = table.scan(ProjectionExpression=projection_expression)
    data = response.get('Items')

    while 'LastEvaluatedKey' in response:
        response = table.scan(
            ProjectionExpression=projection_expression,
            ExclusiveStartKey=response['LastEvaluatedKey'])
        data.extend(response['Items'])

    with table.batch_writer() as batch:
        for each in data:
            batch.delete_item(
                Key={key: each[key] for key in table_key_names}
            )


def tabla_info(logger):
    table = None
    try:
        # Abrimos session para dinamo
        dynamodb = boto3.resource(
            'dynamodb',
            aws_access_key_id=MYKEY,
            aws_secret_access_key=MYSECRET,
            region_name=AWS_REGION
        )
        table = dynamodb.Table(TABLE_NAME)
        logger.info(f'Item count: {table.item_count}')

        dynamodb_client = boto3.client('dynamodb',
                                       aws_access_key_id=MYKEY,
                                       aws_secret_access_key=MYSECRET,
                                       region_name=AWS_REGION)
        table_descr = dynamodb_client.describe_table(
            TableName=TABLE_NAME
        )

        logger.info(f'Tabla decrip: {table_descr}')
        count = table_descr['Table']['ItemCount']
        logger.info(f'Tabla decrip Item count: {count}')


    except Exception as e:
        logger.error(f"Exception general accdediendo a la tabla: {TABLE_NAME}, mensaje: ", exc_info=True)
        table = None
    return table


def extraer_info_imagen(filenames_list, imagen, logger, tipo_imagen):
    dim = calcula_dimensiones_reescalado(imagen)
    # TODO Por que dos inicializaciones???
    target_size = int((min(dim[0], dim[1])) * 0.9)
    target_size = int(min(dim[0], dim[1]))
    # Use ColorThief with the JPEG image
    try:
        # Comprobamos si la imagen es predominantemente clara
        if tipo_imagen == 'gif':
            # Obtenemos el primer frame de la imagen GIF
            frame = Image_pil.open(filenames_list[0])
            is_white = is_predominantly_white(frame)
        else:
            is_white = is_predominantly_white(imagen)

        if is_white:
            dominant_color = (0, 0, 0)
        else:
            color_thief = ColorThief(filenames_list[0])
            # Get the dominant color
            dominant_color = color_thief.get_color(quality=1)

    except Exception as e:
        logger.error("Exception calculando color predominante PNG, mensaje: ", exc_info=True)
        dominant_color = (0, 0, 0)
    return dim, dominant_color, target_size


# TODO ANDRES, revisar las posibilidades de este codigo con cuenta Free...
def obtiene_url_short(domain, url_click):
    # TODO Andres: gestion de keys
    api_key = 'sk_NXkXYJDAKqE5eP88'
    res = requests.post('https://api.short.io/links', json={
        'domain': domain,
        'originalURL': url_click,
    }, headers={
        'authorization': api_key,
        'content-type': 'application/json'
    }, )

    res.raise_for_status()
    data = res.json()

    return data['shortURL']


# Funcion de subida masiva de ficheros de anunciós captuardos a mano a Dynamo DB en parametros_del_modelo
def bulk_load_items(item, table):
    table.put_item(
        Item=item,
    )

    # def process_item(item):
    #     url_imagen_aux = item['url_ad']
    #     url_click_aux = item['url_click']
    #
    #     nombre_imagen_aux = str(obtiene_nombre_fichero(url_imagen_aux))
    #
    #     url_imagen_aux = obtiene_nombre_fichero(item['url_click'])
    #     timestamp_creacion_aux = item['SentTimestamp']
    #
    #     s3_url_imagen = 's3://bitv-ads/' + str(nombre_imagen_aux) + ".img"
    #     s3_url_qr = 's3://bitv-qrs/' + str(nombre_imagen_aux) + ".qr"
    #
    #     # Calculamos el short de Click
    #     domain = '9h5q.short.gy'
    #     url_click_short = obtiene_url_short(domain, url_click_aux)
    #
    #     table.put_item(
    #         Item={
    #             'nombre_imagen': nombre_imagen_aux,
    #             'SentTimestamp': timestamp_creacion_aux,
    #             'url_ad': url_imagen_aux,
    #             'url_click': url_click_aux,
    #             's3_url_imagen': s3_url_imagen,
    #             's3_url_qr': s3_url_qr,
    #             'url_click_short': url_click_short,
    #         },
    #     )

    # TODO Aqui no tiene mucho sentido, solo tendremos un item por cada hilo...
    # Set the maximum number of concurrent workers
    # max_workers = 10
    #
    # # Create a thread pool executor
    # with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    #     # Submit the bulk load tasks to the executor
    #     futures = [executor.submit(process_item, item) for item in items]
    #
    #     # Wait for the tasks to complete
    #     concurrent.futures.wait(futures)
    #
    #     # Retrieve the results (optional)
    #     results = [future.result() for future in futures]
    #
    # return results


# Se adopta el criterio que el nombre del fichero a guardar en nuestro repositorio es el nombre del fichoro que sale
# en la URL quitando el el sufijo final. Este será el indice de la base de datos para luego buscar si de ese anuncio
# ya tenemos escalado en tamaño ese anuncio o no y también sera el nombre de la imagen (***.qr) del QR generado.
def obtiene_nombre_fichero(url_anuncio, logger):
    # Quitamos todos los campos de la URL salvo el ultimo después de la última "/"
    try:
        nombre_fichero_partido = url_anuncio.split('/')
        nombre_fichero = nombre_fichero_partido[-1]

        # Del campo resultante separamos si hay un punto (***.jpeg o ****.gif y nos quedemos con la primera parte)
        nombre_fichero = nombre_fichero.split('.')
        nombre_fichero = nombre_fichero[0]

        # Normalizamos los nombres para evitar caracteres no deseados
        byte_data = nombre_fichero.encode('utf-8')
        encoded_data = base64.urlsafe_b64encode(byte_data)
        nombre_fichero_base64 = encoded_data.decode(encoding="utf-8")
        # Este proceso seria reversible de la siguiente forma:
        # original = base64.urlsafe_b64decode(encoded_data)
        # nombre_original = original.decode(encoding="utf-8")

    except Exception as e:
        logger.error("Exception obteniendo el nombre base64, mensaje: ", exc_info=True)
        nombre_fichero = ""
        nombre_fichero_base64 = ""

    return nombre_fichero, nombre_fichero_base64


# Elimina ficheros en paralelo
def remove_files_with_string(directory, string):
    def remove_file(file_to_remove):
        file_path = os.path.join(directory, file_to_remove)
        os.remove(file_path)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        filenames = os.listdir(directory)
        for filename in filenames:
            if string in filename:
                executor.submit(remove_file, filename)


def get_files_on_s3_resource(session_aws, logger):
    folder_path = '.'
    s3 = boto3.resource('s3',
                        aws_access_key_id=MYKEY,
                        aws_secret_access_key=MYSECRET,
                        region_name=AWS_REGION
                        )
    bucket = s3.Bucket(BUCKET_ADS)
    # folder_objects = list(bucket.objects.filter(Prefix=folder_path))
    folder_objects = bucket.objects.all()
    files_on_s3_ads = []
    for file in folder_objects:
        files_on_s3_ads.append(file.key)

    bucket = s3.Bucket(BUCKET_QRS)
    # folder_objects = list(bucket.objects.filter(Prefix=folder_path))
    folder_objects = bucket.objects.all()
    files_on_s3_qrs = []
    for file in folder_objects:
        files_on_s3_qrs.append(file.key)

    logger.info(f'Elementos en el bucket {BUCKET_ADS}: {len(files_on_s3_ads)}')
    logger.info(f'Elementos en el bucket {BUCKET_QRS}: {len(files_on_s3_qrs)}')
    return
