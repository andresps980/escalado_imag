import argparse
import os.path
import logging


def argumentos_validos():
    parser = argparse.ArgumentParser()

    parser.add_argument("-ll", "--LogLevel", help="Nivel de log", default="INFO")
    parser.add_argument("-dl", "--DirLogs", help="Directorio donde se ubicaran los archivos a tratar, "
                                                 "deberan ser archivos con extension .log", default="./logs/")
    parser.add_argument("-od", "--OutputDir", help="Directorio donde se ubicaran los resultados y "
                                                   "el archivo de trazas", default="./")
    parser.add_argument("--borrar-database", default=False, action="store_true",
                        help="Borrar las entradas en dinamoDB")
    parser.add_argument("--info-database", default=True, action="store_true",
                        help="Informacion sobre tabla en dinamoDB")
    parser.add_argument("--enviar-urls", default=False, action="store_true",
                        help="Enviar Urls a la cola de mensajes para su procesamiento")
    parser.add_argument("--borrar-temps", default=False, action="store_true",
                        help="Borra los archivos temporales del procesamiento de cada imagen")
    parser.add_argument("-dv", "--DiasVidaEntidades", help="Dias que se mantendran las entidades en sus "
                                                           "almacenamientos", default=30)

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

    # TODO Posibilidad de añadir un handle por cada worker y facilitar la lectura de logs...
    # TODO O reducir todo el log de un procesamiento a una linea... para tener solo un archivo y no complicarlo...
    # Create handlers
    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler(args.OutputDir + 'file.log', 'a')
    c_handler.setLevel(dame_nivel_log(args.LogLevel))
    f_handler.setLevel(dame_nivel_log(args.LogLevel))

    # Create formatters and add it to handlers
    c_format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] [%(threadName)s] - %(message)s')
    f_format = logging.Formatter('%(asctime)s - %(name)s - [%(levelname)s] [%(threadName)s] - %(message)s')
    c_handler.setFormatter(c_format)
    f_handler.setFormatter(f_format)

    # Add handlers to the logger
    logger_repos.addHandler(c_handler)
    logger_repos.addHandler(f_handler)
    logger_repos.setLevel(dame_nivel_log(args.LogLevel))

    return logger_repos
