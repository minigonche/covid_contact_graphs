def clean_for_publication(name):
    
    name = name.replace("Palmira Estudio", "Polígono")
    
    return name

def wrap_text(text):
    text = text.strip()
    text = text.split(" ")
    return "\n".join(text)