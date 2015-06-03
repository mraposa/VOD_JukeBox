class GeneralProcessingError(Exception):
    pass
    """
    def __init__(self, value):
        self.value = value
    def __repr__(self):
        return repr(self.value)
        """
    
class CategoryNotFoundError(GeneralProcessingError):
    def __init__(self, value):
        super(GeneralProcessingError, self).__init__(value)
        
class ProductNotFoundError(GeneralProcessingError):
    def __init__(self, value):
        super(GeneralProcessingError, self).__init__(value)

class ProviderNotFoundError(GeneralProcessingError):
    def __init__(self, value):
        super(GeneralProcessingError, self).__init__(value)
        
class ProviderContentTierNotFoundError(GeneralProcessingError):
    def __init__(self, value):
        super(GeneralProcessingError, self).__init__(value)

class DeliverySettingNotFoundError(GeneralProcessingError):
    def __init__(self, value):
        super(GeneralProcessingError, self).__init__(value)

if __name__ == '__main__':
    raise CategoryNotFoundError("CNF error")
    raise GeneralProcessingError("GPE Error")