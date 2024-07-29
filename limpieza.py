import datetime
import boto3

from decimal import Decimal
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.conditions import Attr

from utils.trazas import configura_logs, argumentos_validos
from utils.aws_utils import create_session, dame_tabla_dinamodb, BUCKET_ADS, BUCKET_QRS, TABLE_NAME, TABLE_NAME_URL


def print_cabecera():
    logger.info('')
    logger.info('')
    logger.info('-----------------------------------------------------------------')
    logger.info(' \t Comienza eliminación de entidades de escalado de imagenes')
    logger.info('-----------------------------------------------------------------')


if __name__ == '__main__':
    parser_arg = argumentos_validos()
    args = parser_arg.parse_args()
    print(args)

    logger = configura_logs(args)
    print_cabecera()

    diasVida = args.DiasVidaEntidades
    logger.info(f'Borrado de entidades cuyo ultimo acceso sea hace mas de {diasVida} dias')

    # 86.400 segundos en un dia
    SEG_DIA = 86400
    if diasVida > 365:
        logger.info(f'Limitando el tiempo maximo de permanencia de entidades a 1 año')
        diasVida = 365

    fecha_temp = datetime.datetime.now()
    # Nos vale en segundos
    ts_actual = Decimal(round(fecha_temp.timestamp(), 0))

    # TODO: Para pruebas de lo creado esta mañana
    # ts_desde_borrar = ts_actual - (diasVida * SEG_DIA)
    ts_desde_borrar = ts_actual - 60

    session_aws, sqs, queue_url = create_session(logger)
    table = dame_tabla_dinamodb(logger, session_aws, TABLE_NAME)

    try:
        response = table.scan(
            FilterExpression=Attr('ts_ultimo_acceso').lt(ts_desde_borrar)
        )

    except Exception as err:
        logger.error(
            "Fallo haciendo la consulta de entidades obsoletas. Error: %s: %s",
            err.response["Error"]["Code"],
            err.response["Error"]["Message"],
        )
    else:
        data = response.get('Items')
        # logger.info(data)
        if len(data) > 0:
            s3 = session_aws.client('s3')
            table_url = dame_tabla_dinamodb(logger, session_aws, TABLE_NAME_URL)

        for item in data:
            try:
                nombre_imagen = item['nombre_imagen']
                url_short = item['url_click_short']
                s3_url_qr = item['s3_url_qr']
                s3_url_imagen = item['s3_url_imagen']

                key_s3_imagen = s3_url_imagen.split('/')
                key_s3_imagen = key_s3_imagen[-1]

                key_s3_qr = s3_url_qr.split('/')
                key_s3_qr = key_s3_qr[-1]

                key_url_short = url_short.split('/')
                key_url_short = key_url_short[-1]

                logger.info(f'Eliminando registro nombre_imagen: {nombre_imagen}')

                s3.delete_object(
                    Bucket=BUCKET_QRS,
                    Key=key_s3_qr,
                )

                s3.delete_object(
                    Bucket=BUCKET_ADS,
                    Key=key_s3_imagen,
                )

                resp_url_delete = table_url.delete_item(
                    Key={
                        'short_url': key_url_short
                    }
                )

                resp_delete = table.delete_item(
                    Key={
                        'nombre_imagen': nombre_imagen
                    }
                )
            except Exception as err:
                logger.error(
                    "Fallo eliminando elemento %s. Error: %s: %s",
                    nombre_imagen,
                    err.response["Error"]["Code"],
                    err.response["Error"]["Message"],
                )


