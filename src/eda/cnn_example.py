import matplotlib.pyplot as plt
import numpy as np
from pathlib import Path
import random
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Conv2D, Flatten, Dropout, MaxPooling2D
from tensorflow.keras.preprocessing.image import ImageDataGenerator


# NOTE: This assumes two folders exist within eda containing movingpandas fishing and non_fishing data in the following format:
# /eda/split/train/fishing/
# /eda/split/train/non_fishing/
# /eda/split/test/fishing/
# /eda/split/test/non_fishing/

#Pointing to directory containing train and test daya
train_dir = Path.cwd()/'split'/'train'
test_dir = Path.cwd()/'split'/'test'

# Image dimensions
IMG_HEIGHT = 128
IMG_WIDTH = 128

# Model hyperparameters
batch_size = 256
epochs = 25

# Train generator parameters: Allow for horizontal and vertical transformations
train_image_generator = ImageDataGenerator(rescale=1./255, horizontal_flip=True, vertical_flip=True)

# Generate train images from directory
train_data_gen = train_image_generator.flow_from_directory(batch_size=batch_size,
                                                           directory=train_dir,
                                                           shuffle=True,
                                                           target_size=(IMG_HEIGHT, IMG_WIDTH),
                                                           class_mode='binary',
                                                           color_mode='grayscale')

# Test generator parameters
test_image_generator = ImageDataGenerator(rescale=1./255)

# Generate test images from directory
test_data_gen = test_image_generator.flow_from_directory(batch_size=batch_size,
                                                              directory=test_dir,
                                                              target_size=(IMG_HEIGHT, IMG_WIDTH),
                                                              class_mode='binary',
                                                              color_mode='grayscale')

# Creating basic tf.keras sequential model
model = Sequential([
    Conv2D(8, 3, padding='same', activation='relu', input_shape=(IMG_HEIGHT, IMG_WIDTH ,1)),
    MaxPooling2D(),
    Conv2D(16, 3, padding='same', activation='relu'),
    MaxPooling2D(),
    Conv2D(32, 3, padding='same', activation='relu'),
    MaxPooling2D(),
    Flatten(),
    Dense(256, activation='relu'),
    Dense(1, activation='sigmoid')
])

# Compile the model
model.compile(optimizer='adam',
              loss='binary_crossentropy',
              metrics=['accuracy', 'AUC'])

# Display model architecture
print(model.summary())

# Fit the data using generators and display metrics
history = model.fit_generator(
    train_data_gen,
    epochs = epochs,
    validation_data = test_data_gen
)

# Store fit history for plotting
acc = history.history['accuracy']
val_acc = history.history['val_accuracy']
loss = history.history['loss']
val_loss = history.history['val_loss']
epochs_range = range(epochs)

# Plot the training/test accuracy and loss
plt.figure(figsize=(8, 8))
plt.subplot(1, 2, 1)
plt.plot(epochs_range, acc, label='Training Accuracy')
plt.plot(epochs_range, val_acc, label='Test Accuracy')
plt.legend(loc='lower right')
plt.title('Training and Test Accuracy')

plt.subplot(1, 2, 2)
plt.plot(epochs_range, loss, label='Training Loss')
plt.plot(epochs_range, val_loss, label='Test Loss')
plt.legend(loc='upper right')
plt.title('Training and Test Loss')
plt.show()
