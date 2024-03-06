# Bugs
- Even after extending timeout setting by 30 seconds, clearInterval() is called early
> Solved - By adding clearInterval in timeInterval itself with if condition