import torch
from pytorch_lightning import Trainer

def set_trainer(model, max_epochs=100):
    if torch.backends.mps.is_available():
        print("✅ Usando GPU (MPS - Apple Silicon)")
        model.trainer = Trainer(accelerator="mps", devices=1, max_epochs=max_epochs)
    elif torch.cuda.is_available():
        print("✅ Usando GPU (CUDA)")
        model.trainer = Trainer(accelerator="gpu", devices=1, max_epochs=max_epochs)
    else:
        print("⚠️ Usando CPU")
        model.trainer = Trainer(accelerator="cpu", devices=1, max_epochs=max_epochs)
