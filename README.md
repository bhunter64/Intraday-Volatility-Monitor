# Intraday-Volatility-Monitor
A 3-model intrady volatility monitoring project, centered around using statistical methods to solve quantitative problems in finance.
Built with a dedicated team of undergraduate research students in Math, CS and Engineering.

The three models developed are variations CUSUM, Page-Hinkley, and Baysian Online Change Point Estimation models. Each of these track minute by minute SPY data and alert when a shift in regime (high or low volatility) has occured. The three models are run independently and a voting system is used to combine all three models into one predictive detector. A baseline monitor was also created to compare against the three models. 

This is a QUANTT (Queen's University Algorithmic Network Trading Team) project. It is designed as a tool for Quant Traders and Developers
to inform decisions related to volatility (ie storms, crashes, etc.).

To improve the statistical and mathematical explainability of the model, a research paper is also written in tandem with the tool, to demonstrate
acurate findings and a passion for mathematical research amongst the team.

# Repository Structure
The source files containing the main code for all three statistical models, as well as the baseline model are found in the src/ivtool section of the repo. In order to use the tool perform the actions described in quickstart

All scripts that were used to fetch SPY data, clean the resulting data, and place into csv files are found in the data section of the repo. 

The research section of the repo contains all scripts used for researching and testing new ideas for the three models. It contains various testing notebooks which were created while tuning each of the models as well as some notes and pseudocode explaining how each of the models should work.

Tests contains some of the the testing scripts used to validate the models once they have been created, and is also used to generate some of the plots which display the function of the tool.

The paper written for the project is stored in the paper section of the repository. The paper gives an overview of what the project goals were and what has been accomplished by the team. It includes plots generated through the research and testing phase, and explains in detail how each of the volatility models work 

# quickstart
run the following code in the terminal: python3 -m src.ivtool.pipeline.main_factory
Note: A database url is required to run the code


