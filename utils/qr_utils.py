import segno
import string
import random
import os

from PIL import Image as Image_pil


# GENERACION DE CODIGOS QR de interactividad equivalentes a hacer click en pantalla.
def make_qr(temp_folder, text_, coordenadas_color_, light_='white'):
    qrcode = segno.make(text_)

    # Save the QR code as a PNG file

    # Generate a random string of two characters
    characters = string.ascii_letters
    random_string = ''.join(random.choice(characters) for _ in range(2))

    nombre_fichero_qr_temp = os.path.join(temp_folder, 'qr_temp' + random_string + '.png')

    # qrcode.save('qr_temp.png', scale=10, dark=coordenadas_color_, light = light_ )
    qrcode.save(nombre_fichero_qr_temp, scale=10, dark=coordenadas_color_, light=light_)

    # Devolvemos el valor de nombre ramdom del fichero temporal del qr

    return nombre_fichero_qr_temp


def adjust_qr_to_target_size(qr_file, target_size, temp_folder):
    # Open the PNG file using Pillow

    try:
        qr = Image_pil.open(qr_file)
    except IOError:
        print("Could not open file: " + qr_file)
        return None

    # Generate a random string of two characters
    characters = string.ascii_letters
    random_string = ''.join(random.choice(characters) for _ in range(2))

    # Get the current size of the QR code in pixels
    qr_width, qr_height = qr.size

    # Calculate the aspect ratio of the QR code
    qr_aspect_ratio = qr_width / qr_height

    # Calculate the new dimensions that maintain the aspect ratio and are closest to the target size
    new_width = round(target_size * qr_aspect_ratio)
    new_height = round(target_size / qr_aspect_ratio)

    nombre_fichero_qr_temp = os.path.join(temp_folder, 'qr_temp' + random_string + '.png')

    # Resize the QR code to the new dimensions
    resized_qr_image = qr.resize((new_width, new_height), resample=Image_pil.LANCZOS)

    # Save the resized QR code to file
    # resized_qr_file = os.path.splitext(qr_file)[0] + '_resized.png'
    resized_qr_image.save(nombre_fichero_qr_temp)

    return nombre_fichero_qr_temp
