import cv2 as cv
# from cv2 import cv2_imshow
from cv2 import dnn_superres

import tensorflow as tf

print(cv.__version__)
print(cv.cuda.getCudaEnabledDeviceCount())

# Check the available devices
print("Available devices:")
devices = tf.config.list_physical_devices()

for device in devices:
    print(device)
try:
    tpu = tf.distribute.cluster_resolver.TPUClusterResolver.connect()
    print("Device:", tpu.master())
    strategy = tf.distribute.TPUStrategy(tpu)
except ValueError:
    print("Not connected to a TPU runtime. Using CPU/GPU strategy")
    strategy = tf.distribute.MirroredStrategy()

net = cv.dnn.readNetFromTensorflow("D:\pruebas_repo_mostaza\escalado_imag\models\FSRCNN-small_x2.pb")
net.setPreferableBackend(cv.dnn.DNN_BACKEND_CUDA)
net.setPreferableTarget(cv.dnn.DNN_TARGET_CUDA)

print(cv.cuda.getCudaEnabledDeviceCount())

# Check if CUDA is available
if cv.cuda.getCudaEnabledDeviceCount() > 0:
    # CUDA is available, set the preferable backend to CUDA
    sr = cv2.dnn_superres.DnnSuperResImpl_create()
    sr.setPreferableBackend(cv.dnn.DNN_BACKEND_CUDA)
    sr.setPreferableTarget(cv.dnn.DNN_TARGET_CUDA)
    print("cuda oK")
else:
    # CUDA is not available, fall back to CPU
    sr = cv.dnn_superres.DnnSuperResImpl_create()
    print("CPU OK")
