import sys
import cv2
import cv2 as cv
# from cv2 import cv2_imshow
from cv2 import dnn_superres

import tensorflow as tf

### Read source image
img_src = cv2.imread("D:\pruebas_repo_mostaza\escalado_imag\models\FSRCNN-small_x2.pb")

### Run with GPU: Esto peta por no tener soporte CUDA
# (-216:No CUDA support) The library is compiled without CUDA support in function 'throw_no_cuda'
# img_gpu_src = cv2.cuda_GpuMat()
# img_gpu_dst = cv2.cuda_GpuMat()
# for i in range(100):
#     img_gpu_src.upload(img_src)

print(cv.__version__)
print(cv.cuda.getCudaEnabledDeviceCount())

dev = cv.cuda.getDevice()

cv.cuda.printCudaDeviceInfo(dev)

# Check the available devices
print("Available devices:")
devices = tf.config.list_physical_devices('GPU')
print(f"devices: {devices}")
for device in devices:
    print(f"device: {device}")

logical_gpus = tf.config.experimental.list_logical_devices('GPU')
print(f"logical_gpus: {logical_gpus}")
for device in logical_gpus:
    print(f"device: {device}")

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
