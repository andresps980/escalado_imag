import urllib.request
import numpy as np
import imageio
import string
import random
import os

import cv2
from cv2 import dnn_superres

from io import BytesIO
from PIL import ImageSequence
from PIL import Image as Image_pil
from PIL import ImageFile


def save_gif_frames(image, temp_folder):
    # Open the animated gif with PIL
    with image as im:
        filenames = []
        durations = []
        # Loop through each frame of the gif

        for i in range(im.n_frames):

            # Generamos un par de caracteres random

            # Define the characters you want to choose from
            characters = string.ascii_letters

            # Generate a random string of two characters
            random_string = ''.join(random.choice(characters) for _ in range(2))

            # Seek to the current frame
            im.seek(i)
            # Get the duration of the current frame
            duration = im.info['duration']
            # Generate a unique filename for the current frame
            # filename = f"{os.path.splitext(image)[0]}_{i}.png"
            filename = os.path.join(temp_folder, f"gif_frame_{i}" + random_string + ".png")
            # current_dir = os.path.abspath('.')
            # filename = current_dir+'/'+filename

            # Save the current frame as a png file
            im.save(filename, format='PNG')
            # Append the filename and duration to separate lists
            filenames.append(filename)
            durations.append(duration)

            # TODO Andres se generan gifs de hasta 200 PNGs ?????
            # limitamos las rotaciones del Gif animado a 50
            # if len(filenames) > 20:
            #     # filenames = filenames[0:50]
            #     # durations = durations[0:50]
            #     filenames = filenames
            #     durations = durations

    return filenames, durations


def load_image_from_url(url, temp_folder, logger):
    filenames_list = []
    durations_list = []

    try:
        # Define the characters you want to choose from
        characters = string.ascii_letters
        # Generate a random string of two characters
        random_string = ''.join(random.choice(characters) for _ in range(2))

        with urllib.request.urlopen(url) as response:
            img_data = response.read()

        imagen = Image_pil.open(BytesIO(img_data))

        # deternminamos el tipo de imagen en minúscula
        tipo_imagen = imagen.format.lower()

        # Si la imagen en GIF , la desconompemos en lista de PNGs y tiempos de transición
        if 'gif' in tipo_imagen:
            filenames_list, durations_list = save_gif_frames(imagen, temp_folder)

        elif 'jpeg' in tipo_imagen:  # para el resto del tipo de imágenes
            # generamos un nombre de fichero aleatorio
            filename = os.path.join(temp_folder, "imagen_temp_ " + random_string + ".jpeg")

            # Añadimos el nombre a
            filenames_list = [filename]

            # Save the current frame as a png file
            imagen.save(filename, format='JPEG')

        elif tipo_imagen == 'png':
            # generamos un nombre de fichero aleatorio
            filename = os.path.join(temp_folder, "imagen_temp_ " + random_string + ".png")

            # Añadimos el nombre a
            filenames_list = [filename]

            # Save the current frame as a png file
            imagen.save(filename, format='PNG')
        else:
            imagen = None
            filenames_list = []
            durations_list = []
            tipo_imagen = ""
            logger.error(f"Error tipo imagen no conocido: {tipo_imagen}")

    except Exception as e:
        logger.error(f"Exception cargando imagen desde URL: {url}, message: ", exc_info=True)
        imagen = None
        filenames_list = []
        durations_list = []
        tipo_imagen = ""

    return tipo_imagen, imagen, filenames_list, durations_list


# Esta funcion calcula desde un banner entrante desde ADSERVER de un tamaño estandar IAB de tipo
# IAB (p. ej 780 x 80) hacia un tamaño 1280 de ancho x ** de alto (manteniendo relacion de aspecto)
# En TV la pantalla es de 1280 x 720
# Esta función también tiene una forma de discriminar si el banner es horizontal o vertical
def calcula_dimensiones_reescalado(imagen_):
    # Calculamos las dimensiones de la imagen

    dimensions = imagen_.size

    if dimensions[1] > dimensions[0]:
        factor_escala = (720 / dimensions[1])
        nueva_altura = int(dimensions[1] * factor_escala)
        nueva_anchura = int(dimensions[0] * factor_escala)
        dim = (nueva_anchura, nueva_altura)
    else:
        factor_escala = (1280 / dimensions[0])
        nueva_altura = int(dimensions[1] * factor_escala)
        nueva_anchura = int(dimensions[0] * factor_escala)
        dim = (nueva_anchura, nueva_altura)

    if factor_escala > 2:
        factor_ampliacion = 4
    elif 2 < factor_escala < 4:
        factor_ampliacion = 4
    factor_ampliacion = 4

    return dim


# Función que determina si el color predominante es claro o oscuro
# Así lo que hacemos es que el QR se pueda leer y pase a negro sobre
# blanco si el color predominante es demasiado claro
def is_predominantly_white(imagen_, threshold=200):
    # deternminamos el tipo de imagen en minúscula
    tipo_imagen = imagen_.format.lower()

    if tipo_imagen == "jpeg":
        # Convert the image to HSV color space
        numpy_image = cv2.cvtColor(np.array(imagen_), cv2.COLOR_RGB2BGR)
        hsv_image = cv2.cvtColor(numpy_image, cv2.COLOR_BGR2HSV)

    if tipo_imagen == "gif":
        # Cambiamos la imagen tipo PIL a un numpy array
        numpy_image = cv2.cvtColor(np.array(imagen_), cv2.COLOR_RGB2BGR)
        hsv_image = cv2.cvtColor(numpy_image, cv2.COLOR_BGR2HSV)

    if tipo_imagen == "png":
        # Cambiamos la imagen tipo PIL a un numpy array
        numpy_image = cv2.cvtColor(np.array(imagen_), cv2.COLOR_RGB2BGR)
        hsv_image = cv2.cvtColor(numpy_image, cv2.COLOR_BGR2HSV)

    # Calculate the average value
    average_value = np.mean(hsv_image[:, :, 2])

    # Compare the average value with the threshold
    if average_value > threshold:
        return True  # Predominantly white
    else:
        return False  # Dark enough to be seen against white background


# Función que calcula el tipo de imagen que contiene un fichero
def identify_filetype(image):
    try:
        # Determine file type
        file_extension = image.format.lower()
    except:
        print("Failed to determine image type")
        file_extension = None

    return file_extension


# FUNCION QUE APLICA EL MODELO ELEGIDO DE ESCALADO A LA IMAGEN
# CON INTENTO DE USAR
'''
def async_inference(images):
    with tf.device(device):
        predictions = model2(images, training=False)
    return predictions
'''


# TODO Andres Anotacion solo para notebooks
#  https://www.tensorflow.org/api_docs/python/tf/function#used-in-the-notebooks
# @tf.function
def procedimiento_de_reescalado_imagen_por_ai(modelo_, imagen_):

    # TODO Andres: Elegir donde inicializar lo siguiente
    # TODO Seguramente tengamos que hacer la seleccion a CPU de test_cuda
    sr = cv2.dnn_superres.DnnSuperResImpl_create()

    # with tf.device(device):
    # Obtenemos los parámetros que aplicar al modelo de reescalado
    parametros = calcula_modelo_reescaldo(modelo_)
    sr.readModel(f".\\models\\{parametros[0]}")
    sr.setModel(parametros[1], parametros[2])

    # Cambiamos la imagen tipo PIL a un numpy array
    numpy_image = cv2.cvtColor(np.array(imagen_), cv2.COLOR_RGB2BGR)

    # procesamos la imagen y la reescalamos
    result = sr.upsample(numpy_image)

    # Calculamos las dimensiones de reescalado
    dim_ = calcula_dimensiones_reescalado(imagen_)

    # Reescalamos con CV2 el ultimo tramo hacia abajo de la imagen
    frame_image_resized = cv2.resize(result, dim_, interpolation=cv2.INTER_AREA)

    # Añadimos la imagen a conjunto de imagenes que contiene un gif animado
    # si la imagen no es un gif animado, la imagen es de un únic frame

    # Convert the resized image back to PIL image PNG
    pil_image = Image_pil.fromarray(cv2.cvtColor(frame_image_resized, cv2.COLOR_BGR2RGB))

    # Devolvemos una image PNG PIL
    return pil_image


# Aquí se elige el modelo de reescalado (incremento de imagen) se aplica
# la idea es que primer se escala al factor de escala que tiene fijo cada modelo
#  tamaño x 2 , x3 , x4 y luego el resultante se vuelve a reducir para
# ajustarse a XXXX x 720 (si es vertical) o 1280 x XXXX si es horizonatal
# este banner reescalado se insertará en la L en pantalla.
# Tenemos discriminación y no se usan banners que son casi cuadrados o muy pequeños
# p. ejemplo 320 x 200 o 100 x 100  que son estandares de IAB pero no hay forma de escalarlos
# de forma elegante en una L de TV.
def calcula_modelo_reescaldo(modelo):
    if modelo == 1:
        return "EDSR_x2.pb", "edsr", 2

    if modelo == 11:
        return "EDSR_x4.pb", "edsr", 4

    elif modelo == 2:
        return "FSRCNN_x2.pb", "fsrcnn", 2

    elif modelo == 3:
        return "FSRCNN_x3.pb", "fsrcnn", 3

    elif modelo == 4:
        return "FSRCNN_x4.pb", "fsrcnn", 4

    elif modelo == 5:
        return "FSRCNN-small_x2.pb", "fsrcnn", 2

    elif modelo == 6:
        return "FSRCNN-small_x3.pb", "fsrcnn", 3

    elif modelo == 7:
        return "FSRCNN-small_x4.pb", "fsrcnn", 4

    elif modelo == 8:
        return "FESPCN_x2.pb", "espcn", 2

    elif modelo == 9:
        return "LapSRN_x2.pb", "LapSRN", 2

    elif modelo == 10:
        return "LapSRN_x8.pb", "LapSRN", 8


def make_gif(filenames_, durations_, gif_path_, loop_):
    # Load the images using imageio
    images = [imageio.v2.imread(filename) for filename in filenames_]

    # Set the durations for each frame
    # durations = durations_

    # Convert durations from milliseconds to seconds
    if len(durations_) > 1:
        durations = [duration / 1000 for duration in durations_]
        # Save the animated GIF
        imageio.mimsave(gif_path_, images, duration=durations, loop=loop_)
    else:
        imageio.mimsave(gif_path_, images, duration=5, loop=loop_)

    # Clean up memory by deleting image objects
    del images


def get_total_frames(gif_path):
    # Open the GIF image
    with Image_pil.open(gif_path) as im:
        # Get the total number of frames
        total_frames = im.n_frames

    return total_frames
