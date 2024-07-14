from utils.trazas import configura_logs, argumentos_validos
from utils.aws_utils import create_session, send_message_sqs, dame_tabla_dinamodb, truncateTable, tabla_info, \
    get_files_on_s3_resource


# Obtenemos el total de URLs de anuncios reales captados desde periódicos listado_url_anuncios.txt Este fichero es un
# listado manual urls de anuncios tipo JPEG, GIF, Gif animado, BMP captado a mano desde webs de periódicos. Hoy por
# hoy no es posible tratar anuncios tipo HTML complejos.
def read_text_file(file_path):
    # Open the file and read its contents
    with open(file_path, 'r') as f:
        file_contents = f.read()

    # Clean the file contents by removing any extraneous characters cleaned_contents = re.sub(r'[
    # ^a-zA-Z0-9\s\.\-\_\+\!\@\#\$\%\^\&\*\(\)\[\]\{\}\;\:\'\"\<\>\,\.\?\`\\\/]+', '', file_contents)
    cleaned_contents = file_contents

    # Split the cleaned contents into a list of text strings
    text_strings = cleaned_contents.splitlines()

    return text_strings


def print_cabecera():
    logger.info('')
    logger.info('')
    logger.info('----------------------------------------------------------')
    logger.info(' \t Comienza simulación escalado de imagenes')
    logger.info('----------------------------------------------------------')


if __name__ == '__main__':
    parser_arg = argumentos_validos()
    args = parser_arg.parse_args()
    print(args)

    logger = configura_logs(args)
    print_cabecera()

    session_aws, sqs, queue_url = create_session(logger)

    # Truncado de la tabla dinamoDB usada
    if args.borrar_database:
        table = dame_tabla_dinamodb(logger, session_aws)
        truncateTable(table)

    if args.info_database:
        tabla_info(logger)
        get_files_on_s3_resource(session_aws, logger)

    if args.enviar_urls:
        lista_url_anuncios = read_text_file('D:\pruebas_repo_mostaza\escalado_imag\data\listado_imagenes_errores.txt')
        # lista_url_anuncios = read_text_file('D:\pruebas_repo_mostaza\escalado_imag\data\listado_url_anuncios.txt')
        # lista_url_anuncios = read_text_file('D:\pruebas_repo_mostaza\escalado_imag\data\list2.txt')
        # lista_url_anuncios = read_text_file('D:\pruebas_repo_mostaza\escalado_imag\data\list3.txt')
        logger.info(f'Numero de filas leidas. {len(lista_url_anuncios)}')

        # url = 'https://creatives.sascdn.com/diff/4270/advertiser/503079/300x600_UEM_CHICA_1MAYO.GIF_DFA_e8070fd6-765a' \
        #       '-4778-9c68-44de7fe070f6.gif '
        url_click = 'https://googleads.g.doubleclick.net/aclk?sa=l&ai=CgJ_vVRdhZMjsFvCJnsEPkJCqmAWgwNG2cIOIss_BEfaa' \
                    '-J3NNxABIPKq6Shg1YWAgPQIoAGZp6edA8gBAqkCuEm_tTNnsj7gAgCoAwHIAwiqBIUCT9BCtT9rDRpyq0_GbNmy5GwT' \
                    '-XfozGA0LcyDCjbl_L9-44LM2FV6FNRd8awQZf-dOuL1J3YYtYTkRcOuLI7EnGiwUJc0zN40cjB5tD8eK3pAEEqN62tZa' \
                    '-B8JCbtyNhq4TD7acF6FSnSc5UXIEt6WZrsj4rf61G_sZS2N9WCYaMpacpsA_5iuxXz526AdAhRngZ2mL4' \
                    '-XrrZyk4LkZnRm8g9tmonE-82NeUippaMlneCcDL7nxO3l' \
                    '--9EbPJsaI03KJ5yLvdAJPabLhYBfc_YWWV5Dm0FEByjcc0TLbNwmo3SlWubuV0g65DgpGfleWP5Egxvdl_' \
                    'k4StkTLZXXpIuoVuLwjewASOv7aIpgTgBAGgBgKAB8_Y2GKoB47OG6gHk9gbqAfulrECqAf-nrECqAeko7ECqAfVyRuoB6a' \
                    '-G6gHmgaoB_PRG6gHltgbqAeqm7ECqAeDrbECqAf_nrECqAffn7EC2AcB0ggXCIDhgBAQARgdMgKqAjoDgMADSL39wTqxCUnlLD' \
                    'fZ4kahgAoDmAsByAsBuAwB2BMN0BUBmBYB-BYBgBcB&ae=1&num=1&cid=CAQSiAEAcoEIg_O4GByNZ_uwGL_5c10B9pxLtzrn3' \
                    'Md3uzkR0_9cQ09hMiqq687wmKDBWw1uMb5m-oVHNQQy5rq-UkA3_ht5Vm73I1SHY-9E030KoQfgSKimNK2UDWpbpQr5ccdRhO7' \
                    '_gArpGNMC2QbUoaW0lqGW6ju3hBzidPJ32tnxD9VO2Va6tok8GAE&sig=AOD64_0eB-thj7a0NOPYQD8syGYYtmIq8A&client=' \
                    'ca-pub-8750086020675820&rf=5&nx=CLICK_X&ny=CLICK_Y&uap=UACH(platform)&uapv=UACH(platformVersion)&uaa' \
                    '=UACH(architecture)&uam=UACH(model)&uafv=UACH(uaFullVersion)&uab=UACH(bitness)&uaw=UACH(wow64)&ua' \
                    'fvl=UACH(fullVersionList)&nb=2&adurl=https://campaigns.velasca.com/redirect/collections/tutto' \
                    '-abbigliamento%3Futm_source%3DGoogle_display%26utm_medium%3Dprospecting%26gclid' \
                    '%3DEAIaIQobChMIiOup8qf1_gIV8IQnAh0QiApTEAEYASAAEgJr7PD_BwE '
        # send_message_sqs(sqs, url, url_click, queue_url)

        cont = 1
        for url_demo in lista_url_anuncios:
            # Aceptamos comentarios en el archivo
            if len(url_demo) <= 0 or url_demo[0] == "#":
                continue
            logger.info(f'Mandando mensaje {cont}, url: {url_demo}')
            cont += 1
            send_message_sqs(sqs, url_demo, url_click, queue_url)
