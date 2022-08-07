import numpy as np
from PIL import Image, ImageFilter


image = Image.open(r"post3.png")
image.shape

mx = np.asarray(image)
mx.shape

img = Image.fromarray(mx[400:2425,:, :])

img.save('post3.png', format=None)