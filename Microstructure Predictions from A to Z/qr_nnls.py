import pandas as pd
import numpy as np
import scipy.linalg
import math

def qr_linreg3(x,y,batch_size=400): 
    #x = np.append(np.ones((len(x), 1)), x, axis=1) #if we don't add intercepts beforehand
    m = math.ceil(len(y)/batch_size) 
    n = x.shape[1]
   
    XtX = np.zeros((n,n))
    Xty = np.zeros(n)
    for i in range(m):
        x_split = x[i*batch_size:(i+1)*batch_size]
        y_split = y[i*batch_size:(i+1)*batch_size]
        Q1_i, R1_i = np.linalg.qr(x_split)
        XtX = XtX + np.dot(R1_i.transpose(),R1_i)
        Xty = Xty + np.dot(np.dot(Q1_i,R1_i).transpose(),y_split)
    return XtX,Xty


def nnls_(XtX, Xty, max_iter=50000, tol=1e-8):
    n = XtX.shape[1]
    beta = np.zeros(n)
    for i in range(max_iter):
        g = np.dot(XtX,beta) - Xty
        if np.linalg.norm(g) <= tol: 
            break
        step = np.dot(g,g) / np.dot(g,np.dot(XtX,g))
        beta = np.maximum(beta - step * g, 0)
    return beta


XtX = qr_linreg3(X,y)[0]
Xty = qr_linreg3(X,y)[1]
nnls_(XtX,Xty)

# Verify
# from scipy.optimize import nnls
# nnls(X, y)[0]
