import pandas as pd
import numpy as np


class SignalFactory:
    def __init__(self, weight_factor):
        self.weight_factor = weight_factor

    def compute_book_imbalance(self, bid_vol, ask_vol):
        book_imbalance = (bid_vol - ask_vol) / (bid_vol + ask_vol)
        return book_imbalance

    def compute_weighted_signal(self, data):
        bid_vols = [data['BidQt{}'.format(i)] for i in range(1, 20)]
        ask_vols = [data['AskQt{}'.format(i)] for i in range(1, 20)]
        signal = 0
        for i in range(19):
            book_imbalance = self.compute_book_imbalance(bid_vols[i], ask_vols[i])
            signal += self.weight_factor ** i * book_imbalance
        return signal

signal_generator = SignalFactory(0.5)

import os

folder_path = "/Users/akarberkin/Downloads"
# change this based on the binance folder name
search_phrase = "book_depth_stream"

for file_name in os.listdir(folder_path):
    if file_name.endswith(".parquet") and search_phrase in file_name:
        file_path = os.path.join(folder_path, file_name)
        data = pd.read_parquet(file_path)
        df = pd.DataFrame(data)
        signals = signal_generator.compute_weighted_signal((df))
        print(signals)