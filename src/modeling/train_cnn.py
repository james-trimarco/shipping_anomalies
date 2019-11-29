import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
import random
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
from sklearn.metrics import precision_recall_curve
import tensorflow as tf
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Conv2D, Flatten, Dropout, MaxPooling2D
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.callbacks import EarlyStopping


def run_cnn(split_directory, batchsize=256, epochs=50, color_mode='rgb'):
    
    # Pointing to directory containing train and test data
    # NOTE: This needs to be modified
    train_dir = split_directory/'train'
    test_dir = split_directory/'test'

    # Image dimensions
    IMG_HEIGHT = 128
    IMG_WIDTH = 128
    
    # Check color mode for channels	
    if color_mode=='rgb':	
        IMG_DEPTH = 3	
    else:	
        IMG_DEPTH = 1

    # Model hyperparameters
    batch_size = batchsize
    epoch = epochs

    # Train generator parameters: Allow for horizontal and vertical transformations
    train_image_generator = ImageDataGenerator(rescale=1./255, horizontal_flip=True, vertical_flip=True)

    # Generate train images from directory
    train_data_gen = train_image_generator.flow_from_directory(batch_size=batch_size,
                                                               directory=train_dir,
                                                               shuffle=True,
                                                               target_size=(IMG_HEIGHT, IMG_WIDTH),
                                                               class_mode='binary',
                                                               color_mode=color_mode, 
                                                               classes={'no_fishing': 0, 'fishing': 1})

    # Test generator parameters
    test_image_generator = ImageDataGenerator(rescale=1./255)

    # Generate test images from directory
    test_data_gen = test_image_generator.flow_from_directory(batch_size=batch_size,
                                                             directory=test_dir,
                                                             target_size=(IMG_HEIGHT, IMG_WIDTH),
                                                             class_mode='binary',
                                                             color_mode=color_mode, 
                                                             classes={'no_fishing': 0, 'fishing': 1}, 
                                                             shuffle=False)

    # Creating basic tf.keras sequential model
    model = Sequential([
        Conv2D(8, 3, padding='same', activation='relu', input_shape=(IMG_HEIGHT, IMG_WIDTH ,IMG_DEPTH)),
        Conv2D(8, 3, padding='same', activation='relu'),
        MaxPooling2D(),
        Conv2D(16, 3, padding='same', activation='relu'),
        Conv2D(16, 3, padding='same', activation='relu'),
        MaxPooling2D(),
        Conv2D(32, 3, padding='same', activation='relu'),
        Conv2D(32, 3, padding='same', activation='relu'),
        MaxPooling2D(),
        Flatten(),
        Dense(256, activation='relu'),
        Dense(256, activation='relu'),
        Dense(1, activation='sigmoid')
    ])

    # Early stopping
    earlystop = EarlyStopping(monitor='val_loss', restore_best_weights=True, patience = 4)

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
        validation_data = test_data_gen, callbacks=[earlystop] 
    )

    # Store fit history for plotting
    acc = history.history['accuracy']
    val_acc = history.history['val_accuracy']
    loss = history.history['loss']
    val_loss = history.history['val_loss']
    epochs_range = range(earlystop.stopped_epoch + 1)

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
    plt.savefig('cnn_performance.png')
    plt.close()

    # Reset Test Generator to align with dataset
    test_data_gen.reset()

    # Store data for results csv
    labels = {1: "fishing", 0: "no_fishing"}
    filenames=test_data_gen.filenames
    Y_confidence = model.predict_generator(test_data_gen).flatten()
    Y_pred = Y_confidence.round()
    predictions = [labels[k] for k in Y_pred]
    Y_true = test_data_gen.classes
    true_labels = [labels[k] for k in Y_true]
    
    # Create results csv
    results=pd.DataFrame({"Filename":filenames, 
                          "Confidence":Y_confidence, 
                          "Predictions":predictions, 
                          "True Label": true_labels})
    results.to_csv("results.csv",index=False)
    
    # Store curve details for ROC and PR Curve
    fpr, tpr, thresholds = roc_curve(Y_true, Y_confidence)
    area = auc(fpr, tpr)
    precision, recall, thresholds = precision_recall_curve(Y_true, Y_confidence)
    
    # Plot ROC and PR Curve
    plt.figure(figsize=(8, 8))
    plt.plot([0, 1], [0, 1], 'k--')
    plt.plot(fpr, tpr, label=f'AUC = {area:.3f}')
    plt.xlabel('False positive rate')
    plt.ylabel('True positive rate')
    plt.title('ROC curve')
    plt.legend(loc='best')
    plt.savefig('ROC.png')
    plt.close()

    plt.plot(recall, precision, label=f'AUC = {area:.3f}')
    plt.xlabel('Recall')
    plt.ylabel('Precision')
    plt.title('Precision-Recall Curve')
    plt.legend(loc='best')
    plt.savefig('PR.png')
    plt.close()

    model.save('cnn_model.h5')
    
