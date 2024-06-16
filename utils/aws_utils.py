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

import cv2
from cv2 import dnn_superres

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

TABLE_NAME = 'mostaza_resizing_ads'

# Buckets de S3 a emplear
BUCKET_QRS = 'bitv-qrs'
BUCKET_ADS = 'bitv-ads'


def create_session(logger):
    session_aws = boto3.Session(
        aws_access_key_id=MYKEY,
        aws_secret_access_key=MYSECRET,
        region_name=AWS_REGION)

    # CREACION PARA S3
    s3 = session_aws.client('s3')

    # CREACION PARA DYNAMO_DB
    # specify the table name
    table_name = 'bitv_ads_transform'

    # Create a DynamoDB client using the session
    dynamodb = session_aws.client('dynamodb')

    # CREACION PARA SQS
    queue_name = 'bitv_ad_transform'

    # Initialize the SQS client using the existing session
    sqs = session_aws.client('sqs')

    # Creamos una cola SQS por primera vez sobre AWS (es la cola de anuncios a las que las TVs envían en anuncio si
    # es la primera vez que lo ven, para ser reescalados "off line" y en no tiempo real. Try to get the URL of the
    # queue
    queue_url = None
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        logger.info(f"The queue '{queue_name}' exists and its URL is: {response['QueueUrl']}")
        queue_url = response['QueueUrl']

    except sqs.exceptions.QueueDoesNotExist:
        logger.info(f"The queue '{queue_name}' does not exist.")

        # Create an SQS queue using the existing session
        sqs = session_aws.resource('sqs')
        queue = sqs.create_queue(QueueName=queue_name)
        queue_url = queue.url

        logger.info(f"Created a new queue '{queue_name}' with URL: {queue_url}")

    except ClientError as e:
        logger.error("Exception ClientError aws, creando acceso a colas", exc_info=True)

    except Exception as e:
        logger.error("Exception general, creando acceso a colas", exc_info=True)

    return session_aws, s3, dynamodb, sqs, queue_url


def get_message_body(message):
    message_body = json.loads(message['Body'])
    sent_timestamp = int(datetime.datetime.fromtimestamp(
        int(message['Attributes']['SentTimestamp']) / 1000
    ).timestamp())
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


def process_images(paths, logger):
    bucket_name_imagenes = BUCKET_ADS
    bucket_name_qrs = BUCKET_QRS

    # DEfinimos un dict de los resultados
    dict_resultados = {}

    # List of keys
    keyList = ["nombre_imagen", "timestamp_creacion", "s3_imagen", 's3_qr', 'url_imagen', 'url_click',
               'url_click_short']

    # iterating through the elements of list
    for i in keyList:
        dict_resultados[i] = None

    for path in paths:  # Recorremos todas las imagenes del Bath

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

        try:
            # Intentamos leer la imagen desde la URL recibida
            tipo_imagen, imagen, filenames_list, durations_list = load_image_from_url(url_imagen, temp_folder, logger)
            # tipo_imagen , imagen , filenames_list, durations_list = load_image_from_url_memory(url_imagen)

            # print ("\nmuestra de imagen cargada : ")
            # imagen.show()

        except Exception as e:
            logger.error("Exception calling load_image_from_url, message: ", exc_info=True)
            imagen = None

        if imagen is not None:
            try:
                # Proceso general de UNA imagen del BATCH
                # Preparamos ficheros generales y nombres.
                bucket_s3_imagenes = "s3://" + bucket_name_imagenes
                bucket_s3_qrs = "s3://" + bucket_name_qrs

                # Obtenemos el nombre de fichero de imagen
                nombre_fichero_imagen = obtiene_nombre_fichero(url_imagen)

                # print ('\nnombre_fichero_imagen : ',nombre_fichero_imagen)

                # Obtenemos los nombres de los ficheros a guardar
                nombre_fichero_imagen_a_guardar = nombre_fichero_imagen + '.img'
                nombre_fichero_qr_a_guardar = nombre_fichero_imagen + '.qr'

                # print ("nombre_fichero_imagen_a_guardar : ",nombre_fichero_imagen_a_guardar)
                # print ("nombre_fichero_qr_a_guardar :  ",nombre_fichero_qr_a_guardar)

                nombre_fichero_imagenes_a_guardar_s3 = bucket_s3_imagenes + "/" + nombre_fichero_imagen_a_guardar
                nombre_fichero_qr_a_guardar_s3 = bucket_s3_qrs + "/" + nombre_fichero_qr_a_guardar

                # print ("nombre_fichero_imagenes_a_guardar_s3 : ",nombre_fichero_imagenes_a_guardar_s3)
                # print ("nombre_fichero_qr_a_guardar_s3 : ", nombre_fichero_qr_a_guardar_s3 )

                # Hacemos un proprocesado de los QRs:  las dimensiones y color adecuado a la imagen
                # Obtenemos el valor de la nuevas dimensiones reescaladas para formato TV de la primera imagen de la lista
                # También obtenemos el color dominante para los pixeles del QR pero que resalten sobre fondo blanco

                if tipo_imagen == 'png':

                    dim = calcula_dimensiones_reescalado(imagen)

                    target_size = int((min(dim[0], dim[1])) * 0.9)
                    # print  (target_size)

                    target_size = int(min(dim[0], dim[1]))

                    # Use ColorThief with the JPEG image

                    try:

                        # Comprobamos si la imagen es predominantemente clara
                        is_white = is_predominantly_white(imagen)

                        if is_white:
                            # print("The image is predominantly white.")
                            dominant_color = (0, 0, 0)
                            # print ('dominant_color png  basico',dominant_color)


                        else:
                            # print("The image has a dark enough color to be seen against a mostly white background.")
                            color_thief = ColorThief(filenames_list[0])

                            # Get the dominant color
                            dominant_color = color_thief.get_color(quality=1)
                            # print ('dominant_color png ',dominant_color)

                    except Exception as e:
                        logger.error("Exception calculando color predominante PNG, mensaje: ", exc_info=True)
                        dominant_color = (0, 0, 0)
                        # print ('dominant_color png except',dominant_color)

                if (tipo_imagen == 'gif'):

                    dim = calcula_dimensiones_reescalado(imagen)

                    target_size = int((min(dim[0], dim[1])) * 0.9)
                    # print  (target_size)

                    target_size = int(min(dim[0], dim[1]))

                    # Obtenemos el primer frame de la imagen GIF
                    frame = Image_pil.open(filenames_list[0])

                    try:

                        # Comprobamos si la imagen es predominantement clara
                        is_white = is_predominantly_white(frame)

                        if is_white:
                            # print("The image is predominantly white.")
                            dominant_color = (0, 0, 0)
                            # print ('dominant_color gif  basico',dominant_color)

                        else:
                            # print("The image has a dark enough color to be seen against a mostly white background.")
                            color_thief = ColorThief(filenames_list[0])

                            # Get the dominant color
                            dominant_color = color_thief.get_color(quality=1)
                            # print ('dominant_color gif ',dominant_color)

                    except Exception as e:
                        logger.error("Exception calculando color predominante GIF, mensaje: ", exc_info=True)
                        dominant_color = (0, 0, 0)
                        # print ('gif dominant_color gif except ',dominant_color)

                if (tipo_imagen == 'jpeg'):

                    dim = calcula_dimensiones_reescalado(imagen)

                    target_size = int((min(dim[0], dim[1])) * 0.9)
                    # print  (target_size)

                    target_size = int(min(dim[0], dim[1]))

                    try:

                        # Comprobamos si la imagen es predominantemente clara
                        is_white = is_predominantly_white(imagen)

                        if is_white:
                            # print("The image is predominantly white.")
                            dominant_color = (0, 0, 0)
                            # print ('dominant_color jpeg basico ',dominant_color)


                        else:
                            # print("The image has a dark enough color to be seen against a mostly white background.")
                            color_thief = ColorThief(filenames_list[0])

                            # Get the dominant color
                            dominant_color = color_thief.get_color(quality=1)
                            # print ('dominant_color jpeg  ',dominant_color)
                    except Exception as e:
                        logger.error("Exception calculando color predominante JPEG, mensaje: ", exc_info=True)
                        dominant_color = (0, 0, 0)
                        # print ('dominant_color jpeg except ',dominant_color)

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

                # Calculamos y guardamos el valor de la url_corta con la api de nuestro proveedor externo
                domain = '9h5q.short.gy'
                url_click_short = obtiene_url_short(domain, url_click)

                ###### Creamos el código QR

                # qr_image = make_qr ( url_click_short )

                # Creamos un qr y obtenemos el nombre del fichero donde se ha creado
                nombre_fichero_qr_temp = make_qr(temp_folder, url_click_short, dominant_color, light_='white')

                qr_image = Image_pil.open(nombre_fichero_qr_temp)

                # Ajustamos el QR al tamaño determinado para este tipo

                if target_size > 300:
                    target_size = 300

                # Generamos un QR reescalado y obtnemos el valor del fichero donde se ha depositado
                nombre_fichero_qr_temp_reescalado = adjust_qr_to_target_size(nombre_fichero_qr_temp,
                                                                             target_size,
                                                                             temp_folder)

                # Abrimos la imagen del QR reescalado desde disco
                qr_image_reescalado = Image_pil.open(nombre_fichero_qr_temp_reescalado)

                # Guardamos el QR final con color y reescalado
                nombre_fichero_qr_a_guardar = os.path.join(temp_folder, nombre_fichero_qr_a_guardar)
                qr_image_reescalado.save(nombre_fichero_qr_a_guardar, format='PNG')

                # Mostramos en modo prueba el gif generado con color y tamaño definitivo

                # qr_image_reescalado.show()

                ###########

                # Tenemos que comprobar el tipo de imagen que hemos obtenido
                tipo_imagen = identify_filetype(imagen)

                # print ("tipo de imagen : ", tipo_imagen )

                # si el tipo de imagen es GIF animado, tenemos que ver cuantas images tine
                # Recorvertir una a uno cada frame a tipo admisible por CV2
                # reescarlarlas
                # Volver a montar el GIF animado con los PNGs reescalados

                # TODO Andres: Elegir donde inicializar lo siguiente
                # TODO Seguramente tengamos que hacer la selccion a CPU de test_cuda
                sr = cv2.dnn_superres.DnnSuperResImpl_create()

                if tipo_imagen == 'gif':
                    resized_gif_frames_files = []

                    for file in filenames_list:
                        imagen_aux = Image_pil.open(file)
                        static_image = imagen_aux.convert('RGB')
                        resized_image = procedimiento_de_reescalado_imagen_por_ai(modelo, static_image, sr)
                        resized_image_file = io.BytesIO()
                        resized_image.save(resized_image_file, format='PNG')
                        resized_image_file.seek(0)
                        resized_gif_frames_files.append(resized_image_file)

                    logger.info(f'Total de resized_gif_frames_files: {len(resized_gif_frames_files)}')

                    nombre_fichero_a_guardar = os.path.join(temp_folder, "TONTO_" + nombre_fichero_imagen + '.gif')
                    logger.info(f'Nombre fichero a guardar: {nombre_fichero_a_guardar}')

                    make_gif(resized_gif_frames_files, durations_list, nombre_fichero_a_guardar, 1)
                    logger.info(f'Numero frames imagen final gif: {get_total_frames(nombre_fichero_a_guardar)}')

                    # TODO Andres saltamos esta comprobacion...
                    # Presentamos el gif animado en pantalla
                    # print("Muestra del GIF animado reescalado : ")
                    # display(DisplayImage(filename=nombre_fichero_a_guardar))

                '''
                  if tipo_imagen == 'gif' :

                      resized_gif_frames_files = []

                      # Por este camino vamos a rehacer todas las imagenes del gif al nuevo tamaño.

                      for file in filenames_list :   #lista de todas las imagenes que se compone el gif.

                            # Obtenemos la imagen reescalada con esta función

                            #Cargamos el fichero png del frame

                            #Cargamos el primer frame de GIF animado tipo PNG
                            imagen_aux = Image_pil.open(file)

                            #Convertimos el frame a imagen RGB que pueda usarse en CV2 para reescalado
                            static_image = imagen_aux.convert('RGB')

                            # Obtenemeos una imagén de nuevo PIL PNG escalada al formado adecuado
                            resized_image = procedimiento_de_reescalado_imagen_por_ai (modelo , static_image , sr)

                            # A modo de prueba vemos el resultado de frame intermedio del GIf ya reescalado
                            #print ("Muestra del frame : ", file, " de un GIF ya reescalado ")
                            #resized_image.show()

                            #Guardamos en disco la imagen ampliada que viene y se guarda en PNG
                            #sustituyendo con el nombre del fichero de frame que habíamos leido antes de escalar
                            resized_image.save( file , format='PNG')

                            #lista de ficheros guardado reacondicionados

                            #Añadimos la imagen redimensionada a la lista de imagenes que componen el gif
                            resized_gif_frames_files = resized_gif_frames_files + [file]


                      #Ahora ya tenemos todos los frames del GIF redimensionados y los vamos a remontar en un nuevo gif
                      #Para ello tomamos la lista de frames 'resized_gif_frames' y los tiempos de transición 'durations_list'

                      print ("total de resized_gif_frames_files : ", len(resized_gif_frames_files))

                      # Create and Save the animated GIF como "temp_resized_gif.gif" and LOOP 'ON'

                      # Generate a random string of two characters
                      #characters = string.ascii_letters
                      #random_string = ''.join(random.choice(characters) for _ in range(2))
                      #nombre_fichero_gif_temp = 'temp_resized_gif_'+random_string+'.gif'

                      nombre_fichero_a_guardar = "TONTO_"+nombre_fichero_imagen+'.gif'

                      print (nombre_fichero_a_guardar)

                      %time make_gif ( resized_gif_frames_files , durations_list , nombre_fichero_a_guardar , 0) # 1 = Bucle aninamición = ON
                      #make_gif ( resized_gif_frames_files , durations_list , 'temp_resized_gif.gif'  , 0) # 1 = Bucle aninamición = ON

                      print ("Numero frames imagen final gif : ", get_total_frames( nombre_fichero_a_guardar  ) )

                      #print ("Muestra del GIF animado reescalado : ", im.size )


                      #Presentamos el gif animado en pantalla
                      print ("Muestra del GIF animado reescalado : " )
                      display(DisplayImage(filename= nombre_fichero_a_guardar))
                      '''

                if tipo_imagen == 'png':
                    pass

                    '''
                      #Aplicamos el proceso de reescalado de la imagen a la única que hay en la lista de frames.
                      resized_image = procedimiento_de_reescalado_imagen_por_ai (modelo , imagen , sr)

                      #Guardamos en disco
                      with resized_image as im:
                          nombre_fichero_a_guardar = nombre_fichero_imagen+'.img'
                          # Guardamnos el png
                          im.save(nombre_fichero_a_guardar , format='PNG')


                      #print ("Muestra del fichero PNG reescalado : ",im.size)
                      #im.show()
                      '''

                if tipo_imagen == 'jpeg':
                    pass

                    '''
                      #Aplicamos el proceso de reescalado de la imagen a la única que hay en la lista de frames.
                      resized_image = procedimiento_de_reescalado_imagen_por_ai (modelo , imagen , sr )

                      #Guardamos en disco
                      with resized_image as im:
                          nombre_fichero_a_guardar = nombre_fichero_imagen+'.img'
                          # Guardamnos el png
                          im.save(nombre_fichero_a_guardar , format='PNG')

                      #print ("Muestra del de fichero JPEG reescalado : ", im.size)
                      #im.show()

                      #display(Image_Ipython(filename='temp_resized_jpg.jpeg'))

                      '''

                # Ya tneemos la imagen redimensionada tanto si es un GIF como no
                # También tenemos la imagen del QR generado

                # write resized image + qr image  to S3 buckets
                # s3.put_object(Bucket='mostaza_ads', Key=nombre_fichero_imagen+'.img', Body=resized_image.tobytes())
                # s3.put_object(Bucket='mostaza_qrs_ads', Key=nombre_fichero_imagen+'.qr', Body=qr_image.tobytes())

                # Ahora vamos a guardar los valores en un diccionario transitorio que luego subiremos en bulk a Dynamo
                # Añadimos valores al diccionario

                # Rellenamos los datos a guardar luego en S3

                '''
                  #Añadimos los ficheros de salida
                  lista_ficheros_imagenes_reescaladas = lista_ficheros_imagenes_reescaladas.append(nombre_fichero_imagen+'.img')
                  lista_ficheros_qrs                  = lista_ficheros_qrs.append(nombre_fichero_imagen+'.qr')
                  lista_nombres_ficheros_imagenes_reescaladas = lista_nombres_ficheros_imagenes_reescaladas.append(nombre_fichero_imagen+'.img')
                  lista_nombres_ficheros_qrs          = lista_nombres_ficheros_qrs.append(nombre_fichero_imagen+'.qr')
                  '''

                # Subo los resultados a Dynamo

                Item = {
                    'nombre_imagen': str(nombre_fichero_imagen),
                    'SentTimestamp': timestamp_creacion,
                    'url_ad': url_imagen,
                    'url_click': url_click,
                    's3_url_imagen': 's3://mostaza_ads/' + nombre_fichero_imagenes_a_guardar_s3,
                    's3_url_qr': 's3://mostaza_qrs_ads/' + nombre_fichero_qr_a_guardar_s3,
                    'url_click_short': url_click_short,
                },

                # TODO Andres: De momento no subimos las imagenes hasta controlar la prueba...
                # bulk_load_items(Item, table)

                ########

                # Procedemos a la subida a S3 de los ficheos de imagen

                '''
                  print ("\n---'s3://mostaza_ads/'+nombre_fichero_imagenes_a_guardar_s3---")
                  print (nombre_fichero_imagenes_a_guardar_s3)


                  print ("-------s3://mostaza_qrs_ads/'+nombre_fichero_qr_a_guardar_s3------")
                  print (nombre_fichero_qr_a_guardar_s3)


                  print ("-----nombre_fichero_a_guardar -----")
                  print (nombre_fichero_a_guardar)

                  print ("-----nombre_fichero_qr_a_guardar-----")
                  print (nombre_fichero_qr_a_guardar)
                  '''

                '''
                  #########

                  #Sumimos imagen reescalada a S3

                  nombre_fichero_a_subir = nombre_fichero_a_guardar
                  nombre_fichero_salida = nombre_fichero_a_guardar

                  BUCKET_imagenes = 'bitv-ads'

                  upload_file( nombre_fichero_a_subir , BUCKET_imagenes , object_name = nombre_fichero_salida )

                  #########


                  #Subimos qr reescalado a S3

                  nombre_fichero_a_subir = nombre_fichero_qr_a_guardar
                  nombre_fichero_salida = nombre_fichero_qr_a_guardar

                  BUCKET_qr = 'bitv-qrs'

                  upload_file( nombre_fichero_a_subir , BUCKET_qr , object_name = nombre_fichero_salida )
                  '''

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

    # print (data['shortURL'])

    return data['shortURL']


# Funcion de subida masiva de ficheros de anunciós captuardos a mano a Dynamo DB en parametros_del_modelo
def bulk_load_items(items, table):
    def process_item(item):
        url_imagen_aux = item['url_ad']
        url_click_aux = item['url_click']

        nombre_imagen_aux = str(obtiene_nombre_fichero(url_imagen_aux))

        url_imagen_aux = obtiene_nombre_fichero(item['url_click'])
        timestamp_creacion_aux = item['SentTimestamp']

        s3_url_imagen = 's3://bitv-ads/' + str(nombre_imagen_aux) + ".img"
        s3_url_qr = 's3://bitv-qrs/' + str(nombre_imagen_aux) + ".qr"

        # Calculamos el short de Click
        domain = '9h5q.short.gy'
        url_click_short = obtiene_url_short(domain, url_click_aux)

        table.put_item(
            Item={
                'nombre_imagen': nombre_imagen_aux,
                'SentTimestamp': timestamp_creacion_aux,
                'url_ad': url_imagen_aux,
                'url_click': url_click_aux,
                's3_url_imagen': s3_url_imagen,
                's3_url_qr': s3_url_qr,
                'url_click_short': url_click_short,
            },
        )

    # Set the maximum number of concurrent workers
    max_workers = 10

    # Create a thread pool executor
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit the bulk load tasks to the executor
        futures = [executor.submit(process_item, item) for item in items]

        # Wait for the tasks to complete
        concurrent.futures.wait(futures)

        # Retrieve the results (optional)
        results = [future.result() for future in futures]

    return results


# Se adopta el criterio que el nombre del fichero a guardar en nuestro repositorio es el nombre del fichoro que sale
# en la URL quitando el el sufijo final. Este será el indice de la base de datos para luego buscar si de ese anuncio
# ya tenemos escalado en tamaño ese anuncio o no y también sera el nombre de la imagen (***.qr) del QR generado.
def obtiene_nombre_fichero(url_anuncio):
    # Quitamos todos los campos de la URL salvo el ultimo después de la última "/"
    try:
        nombre_fichero_partido = url_anuncio.split('/')
        nombre_fichero = nombre_fichero_partido[-1]

        # Del campo resultante separamos si hay un punto (***.jpeg o ****.gif y nos quedemos con la primera parte)
        nombre_fichero = nombre_fichero.split('.')
        nombre_fichero = nombre_fichero[0]

    except:
        nombre_fichero = ""

    return nombre_fichero


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
