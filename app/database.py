class list_data:
    def __init__(self,key,value,exp_time = 0):
        self.key = key
        self.value = value
        self.exp_time = exp_time
    
    def get_value(self):
        return self.value

class RPUSH:
    
    def __init__(self,name):
        self.name = name
        self.elements = []

    
    def append_list(self,elements):
        self.elements.append(elements)
        return None
        
    
    def get_name(self):
        return self.name

    def get_elements(self,i =  None):
        if i is None:
            return self.elements
        return self.elements[i]
    
    def get_element_length(self):
        return len(self.elements)


    