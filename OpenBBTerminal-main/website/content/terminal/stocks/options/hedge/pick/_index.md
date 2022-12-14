```
usage: long [-p PICK [PICK ...]] [-a AMOUNT] [-h]

This function plots option hedge diagrams

optional arguments:
  -p PICK [PICK ...], --pick PICK [PICK ...]
                        Choose what you would like to pick (default: None)
  -a AMOUNT, --amount AMOUNT
                        Choose the amount invested (default: 1000)
  -h, --help            show this help message (default: False)
```

Example
```
2022 May 10, 09:21 (๐ฆ) /stocks/options/hedge/ $ pick 170 Short Call -a 500
2022 May 10, 09:22 (๐ฆ) /stocks/options/hedge/ $ help
โญโโโโโโโโโโโโโโโโโโโโโโStocks - Options - Hedge โโโโโโโโโโโโโโโโโโโโโโโโโโฎ      
โ Ticker:                                                                |
AAPL                                                                     |
โ Expiry:                                                                | 2022-05-13          |                                                                         
โ     pick          pick the underlying asset position   
โ                                                                                  
โ Underlying Asset Position: Short Call 500 @ 170 
โ  
โ     list          show the available strike prices for calls and puts 
โ     add           add an option to the list of options  
โ     rmv           remove an option from the list of options 
โ     sop           show selected options and neutral portfolio weights 
|
โฐโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโโOpenBB Terminal โโฏ
```