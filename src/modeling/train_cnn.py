import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pathlib import Path
import random
from sklearn.metrics import roc_curve
from sklearn.metrics import auc
from sklearn.metrics import precision_recall_curve
import tensorflow as tf
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Input, Conv2D, MaxPooling2D, Dense, Flatten, BatchNormalization
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.callbacks import EarlyStopping

def Conv_block(layer_in, n_filters, n_conv, batchnorm = False):
    
    for _ in range(n_conv):
        # Conv Layers
        layer_in = Conv2D(n_filters, (3,3), padding='same', activation='relu')(layer_in)
        if batchnorm:
            layer_in = BatchNormalization()(layer_in)
    # Max Pooling
    layer_in = MaxPooling2D((2,2), strides=(2,2))(layer_in)
    return layer_in

def Dense_block(layer_in, n_neurons,n_dense):
    for _ in range(n_dense):
        layer_in = Dense(n_neurons, activation='relu')(layer_in)
    return layer_in

def run_cnn(split_directory, batchsize=256, epochs=50, color_mode='rgb',start_filters=8, depth=2, dense_count = 2, dense_neurons = 256, bnorm = False):
    
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
    
    if start_filters > 32:
        start_filters = 32
        print("Too many starting filters. Restricting to 32.")
    elif start_filters < 8:
        start_filters = 8
        print("Too few starting filters. Restricting to 8.")
    
    if depth > 3:
        depth = 3
        print("Model too deep for this image size. Restricting to depth of 3.")
    elif depth < 1:
        depth = 1
        print("Model too shallow for this image size. Restricting to depth of 1.")
        
    if dense_count > 2:
        dense_count = 2
        print("Unorthodox dense layer. Restricting to dense layer count of 2.")
    elif dense_count < 1:
        dense_count = 1
        print("Unorthodox dense layer. Restricting to dense layer count of 1.")
        
    if dense_neurons > 512:
        dense_neurons = 512
        print("Too much compute. Restricting to neuron count of 512.")
    elif dense_neurons < 64:
        dense_neurons = 64
        print("Too few neurons. Restricting to neuron count of 64.")
        
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

    # Creating tf.keras functional model
    image_input = Input(shape=(IMG_HEIGHT, IMG_WIDTH , IMG_DEPTH))
    layer = Conv_block(image_input, start_filters, 2, batchnorm=bnorm)
    for i in range(depth):
        layer = Conv_block(layer, start_filters*(2*(i+1)), 2, batchnorm=bnorm)
    layer = Flatten()(layer)
    layer = Dense_block(layer, dense_neurons,dense_count)
    layer = Dense(1, activation='sigmoid')(layer)
    model = Model(inputs=image_input, outputs=layer)

    # Early stopping
    earlystop = EarlyStopping(monitor='val_loss', restore_best_weights=True, patience = 8)

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
    
