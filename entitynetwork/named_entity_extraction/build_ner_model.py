from deeppavlov import configs, build_model
from entitynetwork.helper_classes.warning_suppression import suppress_stdout

def select_model(model_name=None):
    transformed_model_name = "configs.ner." + model_name
    return transformed_model_name


def load_model(model_name=None, download=True):
    selected_model = select_model(model_name)
    with suppress_stdout():
        ner_model = build_model(eval(selected_model), download=download)
    return ner_model