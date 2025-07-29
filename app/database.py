from collections import deque
class list_data:
    def __init__(self,key,value,exp_time = 0):
        self.key = key
        self.value = value
        self.exp_time = exp_time
    
    def get_value(self):
        return self.value

class PUSH:
    
    def __init__(self,name):
        self.name = name
        self.elements = deque()

    def get_name(self):
        return self.name

    def get_elements(self,i =  None):
        if i is None:
            return self.elements
        return self.elements[i]
    
    def get_element_length(self):
        return len(self.elements)
        
    def append_right(self,elements):
        self.elements.append(elements)
        return None
    
    def append_left(self,elements):
        self.elements.appendleft(elements)
        return None
    
    def pop_left(self):
        remove = self.elements.popleft()
        return remove
    

    


    