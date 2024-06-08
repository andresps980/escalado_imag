import logging
import argparse
import os.path

from utils.aws_utils import create_session, get_messages_from_sqs_parallel, process_images


def argumentos_validos():
    parser = argparse.ArgumentParser()

    parser.add_argument("-ll", "--LogLevel", help="Nivel de log", default="INFO")
    parser.add_argument("-dl", "--DirLogs", help="Directorio donde se ubicaran los archivos a tratar, deberan ser "
                                                 "archivos con extension .log", default="./logs/")
    parser.add_argument("-od", "--OutputDir", help="Directorio donde se ubicaran los resultados y el archivo de trazas",
                        default="./")

    return parser


def dame_nivel_log(level):
    niveles = {'DEBUG': logging.DEBUG,
               'INFO': logging.INFO,
               'WARNING': logging.WARNING,
               'ERROR': logging.ERROR,
               'CRITICAL': logging.CRITICAL}
    return niveles[level]


def configura_logs(args):
    if not os.path.exists(args.OutputDir):
        os.mkdir(args.OutputDir)

    logger_repos = logging.getLogger(__name__)

    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(args.OutputDir + 'file.log', 'a')
    c_handler.setLevel(dame_nivel_log(args.LogLevel))
    f_handler.setLevel(dame_nivel_log(args.LogLevel))

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger_repos.addHandler(c_handler)
    logger_repos.addHandler(f_handler)
    logger_repos.setLevel(dame_nivel_log(args.LogLevel))

    return logger_repos


def print_cabecera():
    logger.info('')
    logger.info('')
    logger.info('----------------------------------------------------------')
    logger.info(' \t Comienza procesamiento de logs escalado de imagenes')
    logger.info('----------------------------------------------------------')


if __name__ == '__main__':
    parser_arg = argumentos_validos()
    args = parser_arg.parse_args()
    print(args)

    logger = configura_logs(args)
    print_cabecera()

    # Bucle infinito de funcionamiento
    # Tiempo de inicio de proceso del batch de mensajes SQS
    start_time = time.time()

    # # CREACION PARA S3
    # global s3
    # s3 = session_aws.client('s3')
    # # Create a DynamoDB client using the session
    # global dynamodb
    # dynamodb = session_aws.client('dynamodb')

    session_aws, s3, dynamodb, sqs = create_session()

    # Get a list of all messages from sqs queue from TVs
    # Este será nuestro Batch
    # num_max_messages = len(mensajes)
    num_max_messages = 100

    # Origen de los ficheros de datos
    file_list_json = get_messages_from_sqs_parallel(queue_url, num_max_messages)
    # file_list_json  = mensajes[0:10]
    # file_list_json  = mensajes

    print("Número de ficheros ", len(file_list_json))

    # Print the list of files
    print("Número de imágenes ", len(file_list_json))

    # Get image paths
    # image_paths = ['imagen{}.jpeg'.format(i) for i in range(1000)]
    # image_paths = file_list

    batch_size = 40

    # Split json lists into batches
    batches = [file_list_json[i:i + batch_size] for i in range(0, len(file_list_json), batch_size)]

    print("Número de batches ", len(batches))

    #################

    # Create a process pool with one process per CPU core
    max_workers = 20
    # max_workers = 1

    global contador
    contador = 1

    # Create a thread pool executor

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        # with concurrent.futures.ProcessPoolExecutor() as executor:
        # Apply process_images function to each batch of image paths asynchronously
        results = [executor.submit(process_images, batch) for batch in batches]

        # Get the results
        # resized_images = [result.result() for result in concurrent.futures.as_completed(results)]

    # Flatten the resized images list
    # resized_images = [image for batch in resized_images for image in batch]

    # print(len(resized_images))

    # Print the total processing time
    print("--- %s seconds ---" % (time.time() - start_time))

'''
with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
    # Iterate over the batches of image paths
    for batch in batches:
        # Apply process_images function to the current batch
        future = executor.submit(process_images, batch)

        # Process the results of each batch as they become available
        # This allows processing of images in a streaming fashion, reducing memory consumption
        resized_images = future.result()

        # Process the resized_images (e.g., save or perform further computations) as needed

        # Release memory of resized_images
        del resized_images
'''

# Print the total processing time
print("--- %s seconds ---" % (time.time() - start_time))
