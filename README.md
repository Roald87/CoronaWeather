# Corona weather effects analyses
Analysis of the weather in the Netherlands during the 2020 corona crisis. See also the [blog post](https://roald87.github.io/python/2020/04/12/corona-weather.html).

Inspired by this [article](https://www.nature.com/articles/418601a) I was curious if there was any effect on the weather of the reduced air traffic.

I analyzed data from the Netherlands and didn't really find a large effect for the temperature differences as they did in the article. However, I did find that it was awfully sunny!

![](sun_hours_netherlands.png)
Source: [KNMI](https://www.knmi.nl/kennis-en-datacentrum/achtergrond/data-ophalen-vanuit-een-script).

For more details see the Jupyter notebooks for [🇳🇱 NL](CoronaWeather_NL.ipynb) and [🇨🇭 CH](CoronaWeather_CH.ipynb) or rerun the notebook in Colab ([🇳🇱 NL](https://colab.research.google.com/github/Roald87/CoronaWeather/blob/master/CoronaWeather_NL.ipynb#) and [🇨🇭 CH](https://colab.research.google.com/github/Roald87/CoronaWeather/blob/master/CoronaWeather_CH.ipynb#)) to see the latest data. 

Note: the Swiss Colab notebook sometimes skips cells. To fix this just run the cells one by one or rerun the ones starting from the one which was skipped. For the Dutch Colab one the plots are often not shown correctly on the first run. Just rerun the plot cells to render them. 
