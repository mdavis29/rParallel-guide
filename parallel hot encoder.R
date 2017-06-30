
library(doParallel)  
library(Matrix)

 data("Titanic")
 rd<-data.frame(Titanic)


 ### one hot encoder that runs in parallel
parallelOneHot<-function(mydata, varCols = NULL,
                                  sparse = FALSE, 
                                  oneHot = TRUE, 
                                  verbose = FALSE){
  if(is.null(varCols)){
    varCols <- colnames(mydata)[lapply(mydata, class) == 'factor']
    if(verbose)print(varCols)
  }
  output<-NULL
  if(verbose)(print(paste('varCols', varCols)))
  for ( i in 1:(length(varCols))){
    tempOut<-oneHotVector(mydata[, varCols[i]],
                          oneHot = oneHot, 
                          verbose = verbose, 
                          sparse = sparse,
                          colNamePrefix = varCols[i] ) 
    if(verbose)print(dim(tempOut))
    output<-cbind(output, tempOut)
    
  }
  return(output)
}
  

  
### one hot encode a signle vector 
  oneHotVector<-function(x, levs = NULL, 
                         oneHot = TRUE, 
                         minFreq = 1, 
                         verbose = FALSE, 
                         colNamePrefix = NULL,
                         sparse = FALSE
                         ){
    output<-NULL
    if(is.null( levs)){
      if(verbose)print('building custom levels')
      tab <- table(x)
      tab <- tab[order(tab)]
      if(!is.null(minFreq)){
        tab<-tab[tab >= minFreq]
        if(verbose)print(paste('using min freq', minFreq))
        }
      allLevels <- names(tab)
      if(oneHot){
        if(verbose)print('using one hot')
        allLevels<-tail(allLevels, length(allLevels)-1)
      }
    }
    if(!is.null(levs)){allLevels<-levs}
    if(verbose)print(allLevels)
    library(doParallel)  
    n<-detectCores()
    registerDoParallel(n)
    output = foreach(i=1:length(allLevels),.combine = 'cbind' ) %dopar% {  
      ifelse(x == allLevels[i], 1,0 )
    }
    output[is.na(output)]<-0
    if(!sparse) output<-matrix(as.numeric(output), ncol = length(allLevels))
    if(sparse) output<-Matrix(as.numeric(output), ncol = length(allLevels), sparse = TRUE)
    colnames(output)<-allLevels
    if(!is.null(colNamePrefix)){colnames(output)<-paste(colNamePrefix,colnames(output), sep = '.' )}
    return(output)
  }
  oneHotVector(rd[,2],sparse = TRUE)
  
  parallelOneHot(rd, verbose = TRUE, sparse = TRUE)
  
  
  
  
  