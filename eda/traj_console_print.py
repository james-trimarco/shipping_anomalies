import numpy as np
from pathlib import Path
from PIL import Image


# REPURPOSED FROM: https://github.com/nikhilkumarsingh/terminal-image-viewer


def get_ansi_color_code(r, g, b):
    """ 
    Converts rgb values in numpy array to ansi for console.
  
    Parameters: 
    r (int): Red-channel pixel value
    g (int): Green-channel pixel value
    b (int): Blue-channel pixel value
  
    Returns: 
    ansi_color_code (int): Code for console color 
  
    """
    if r == g and g == b:
        if r < 8:
            return 16
        if r > 248:
            return 231
        return round(((r - 8) / 247) * 24) + 232
    return 16 + (36 * round(r / 255 * 5)) + (6 * round(g / 255 * 5)) + round(b / 255 * 5)


def get_color(r, g, b):
    """ 
    Inserts color code into string for console printing.
  
    Parameters: 
    r (int): Red-channel pixel value
    g (int): Green-channel pixel value
    b (int): Blue-channel pixel value
  
    Returns: 
    ansi_string (str): String needed to print color in console
  
    """
    return f"\x1b[48;5;{int(get_ansi_color_code(r,g,b))}m \x1b[0m"


def show_image(flat_trajectory, resize = True):
    """ 
    Takes a trajectory and prints into console.
  
    Parameters: 
    flat_trajectory (list): List containing trajectory of boat
    resize (bool): Resizes printing for readability
  
    """
    
    # Attempt converting from flat trajectory to image
    try:
        img = flat_trajectory
        
        # Modular way to determine resizing for square image
        reshape_dim = int(len(img)**(1./2))
        
        # Converts flat array to grayscale
        grayscale_img = np.where(np.array(img)>0,255,0).reshape(-1, reshape_dim)
        
        # Converts grayscale image to rgb
        rgb_img = np.stack((grayscale_img,)*3, axis=-1)
    
    # EXIT IF TRAJECTORY ERROR
    except:
        exit('Trajectory format INCORRECT')
    
    # Halve the size of print output for readibility
    if resize:
        h = int(reshape_dim*.5)
        w = h
        rgb_img.resize((h,w,3))

    # Obtain final dimensions of image
    h,w,c = rgb_img.shape

    # For every height component
    for x in range(h):
        
        # For every width component
        for y in range(w):
            
            # Obtain pixel rgb values
            pix = rgb_img[x][y]
            
            # Print in ansi color scheme for console
            print(get_color(pix[0], pix[1], pix[2]), sep='', end='')
            print(get_color(pix[0], pix[1], pix[2]), sep='', end='')
        print()


if __name__ == '__main__':
    example = np.random.randint(low=0, high=2, size=4096).tolist()
    show_image(example)
