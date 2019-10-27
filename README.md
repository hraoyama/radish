# radish
Backtesting and research  platform

## Required

### For radish framework

   1) install the following:

   * Anaconda python 3.6+ with pyCharm IDE and jupyter notebook
   * https://arctic.readthedocs.io/en/latest/ (will require mongodb installation as well but detailed in above)
   * https://redis.io/
   * https://www.atlassian.com/git/tutorials/install-git
   * https://confluence.atlassian.com/get-started-with-sourcetree/install-sourcetree-847359094.html

   2) get a free github account
   3) get to know some github: https://www.youtube.com/watch?v=HVsySz-h9r4

### For notebooks

   1) install R (rproject.org), make sure to have an ```R_HOME``` environment variable (e.g. C:\Program Files\R\R-3.4.3) and an ```R_PATH``` environment variable (e.g. %R_HOME%\bin\x64).
   2) install Rtools. In R:
    ```
    install.packages(installr);require(installr);install.Rtools() 
    ```
    see https://www.rdocumentation.org/packages/installr/versions/0.22.0/topics/install.Rtools
    
   3) Edit your enviroment variables to have an R_TOOLS variable to have the Rtools path (e.g. C:\Rtools) and an R_TOOLS_PATH with the rtools subdirectories (e.g. %R_TOOLS%\bin;%R_TOOLS%\gcc-4.6.3\bin)

   4) open an anaconda prompt and install the relevant packages:```pip install rtools``` followed by ```conda install rpy2```.
    rpy2 does not seem to play well with the pip installation, conda seems to work fine for some reason.

   5) open a python notebook and execute:```%load_ext rpy2.ipython```. The default for the notebook is python, if you want to use R the new cell in the notebook should start with ```%%R```.
    
    ```
    %load_ext rpy2.ipython
    import pandas as pd
    df = pd.DataFrame({'cups_of_coffee': [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],'productivity': [2, 5, 6, 8, 9, 8, 0, 1, 0, -1]})
    ```
   ```
   %%R -i df -w 5 -h 5 --units in -r 200
   install.packages("ggplot2",quiet=TRUE)
   library(ggplot2)
   ggplot(df, aes(x=cups_of_coffee, y=productivity)) + geom_line()
   ```
   The second cell imports ```df```  from global environment and makes the default figure size 5 by 5 inches with 200 dpi resolution.



   




 
