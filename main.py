import time
import concurrent.futures

from utils.aws_utils import create_session, get_messages_from_sqs_parallel, process_images
from utils.trazas import configura_logs, argumentos_validos


def print_cabecera():
    logger.info('')
    logger.info('')
    logger.info('----------------------------------------------------------')
    logger.info(' \t Comienza proceso escalado de imagenes')
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

    session_aws, s3, dynamodb, sqs, queue_url = create_session(logger)
    if queue_url is None:
        exit(-1)

    while True:
        # Get a list of all messages from sqs queue from TVs
        # Este será nuestro Batch
        # num_max_messages = len(mensajes)
        num_max_messages = 100

        # Origen de los ficheros de datos
        file_list_json = get_messages_from_sqs_parallel(sqs, queue_url, num_max_messages, logger)
        # file_list_json  = mensajes[0:10]
        # file_list_json  = mensajes

        logger.info(f"Número de ficheros e imagenes {len(file_list_json)}")

        if len(file_list_json) == 0:
            logger.info(f'No hay mensajes en cola, 5 segundos de espera...')
            time.sleep(5)
            continue

        # Get image paths
        # image_paths = ['imagen{}.jpeg'.format(i) for i in range(1000)]
        # image_paths = file_list

        batch_size = 40

        # Split json lists into batches
        batches = [file_list_json[i:i + batch_size] for i in range(0, len(file_list_json), batch_size)]

        logger.info(f"Número de batches {len(batches)}")

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
            results = [executor.submit(process_images, batch, logger) for batch in batches]

            # Get the results
            # resized_images = [result.result() for result in concurrent.futures.as_completed(results)]

        # Flatten the resized images list
        # resized_images = [image for batch in resized_images for image in batch]

        # print(len(resized_images))

        # Print the total processing time
        logger.info(f"Tiempo de procesamiento: {(time.time() - start_time)} seconds")

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
