cat $1 | awk '{ if ( $1 == "Sharpe" ) { sharpe = $4; } if ( ( $1=="Net" ) && ( $2=="Returns" ) ) { netret = $4; } if ( $1=="Annualized_Returns" ) { annret=$NF;} if ( $1=="Annualized_Std_Returns" ) { annstd=$NF;} if ( $1=="Max" && $2=="Drawdown" &&$3=="=") { maxdd=$NF;} if ( $1=="Return_drawdown_Ratio" ) { retbydd=$NF;} if ( $2=="Var10" ) { retvar10=$NF;} if ( $1=="Skewness" ) { skew=$NF;} if ( $1=="Kurtosis" ) { kurt=$NF;} if ( $1=="DML" ) { dml=$NF;} if ( $1=="MML" ) { mml=$NF;} if ( $1=="QML" ) { qml=$NF;} if ( $1=="YML" ) { yml=$NF;} if ( $1=="Turnover" ) { turnover=$NF;} if ( $2=="Cost" ) { tcost=$NF;} if ( $2=="Money" ) { tmoney=$NF;} if ( $2=="Orders" ) { orders=$NF;} } END{ print sharpe,netret,annret,annstd,maxdd,retbydd,retvar10,skew,kurt,dml,mml,qml,yml,turnover,tcost,tmoney,orders }'
