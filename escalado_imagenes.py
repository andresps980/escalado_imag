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

    session_aws, sqs, queue_url = create_session(logger)
    if queue_url is None:
        exit(-1)

    while True:
        num_max_messages = 100
        file_list_json = get_messages_from_sqs_parallel(sqs, queue_url, num_max_messages, logger)
        logger.info(f"Número de ficheros e imagenes {len(file_list_json)}")

        if len(file_list_json) == 0:
            logger.info(f'No hay mensajes en cola, 5 segundos de espera...')
            time.sleep(5)
            continue

        start_time = time.time()

        # TODO elegir/configurar el numero de elementos por batch
        # Split json lists into batches
        batch_size = 10
        batches = [file_list_json[i:i + batch_size] for i in range(0, len(file_list_json), batch_size)]
        logger.info(f"Número de batches {len(batches)}")

        # Create a process pool with one process per CPU core
        max_workers = 10
        # Create a thread pool executor
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix='process_images') as executor:
            # Apply process_images function to each batch of image paths asynchronously
            results = [executor.submit(process_images, batch, logger, session_aws, args.borrar_temps) for batch in batches]

        # Print the total processing time
        logger.info(f"Tiempo de procesamiento: {(time.time() - start_time)} seconds")
