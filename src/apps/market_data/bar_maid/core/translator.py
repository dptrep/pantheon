'''
translate symbols based on corporate actions

by: Dan Trepanier

Jun 11, 2022
'''


class Translator(object):
    def __init__(self):
        self.symbols = {'FB':'META',
                        'META':'FB'
                        }
        self.new = {'FB': {'date': '20220609', 'symbol':'META'},
                        }
        self.old = {'META': {'date': '20220608', 'symbol': 'FB'}}
    
    def get(self, symbol, date):
        if symbol in self.new:
            x = self.new[symbol]
            if date >= x['date']:
                return x['symbol']
        if symbol in self.old:
            x = self.old[symbol]
            if date <= x['date']:
                return x['symbol']
        return symbol

    def clean_list(self,symbols):
        new = []
        for symbol in symbols:
            if symbol in self.symbols:
                new += [self.symbols[symbol]]
            new += [symbol]
        return new