defmodule Pastry do
  use GenServer
  def main(args) do
    arguments = parse_args(args)
    
    no_of_req = elem(arguments,1) 
    input_val = elem(arguments,0)
    
    list = getNodeList(input_val)
    startNodes(list, list, no_of_req)
    callStartSearching(list)
    
    checkForCompletion(list,input_val*no_of_req,0,0)
    print_state(list,0,0)
  end

  def checkForCompletion(list,expectedCount,preCount,sameCount) do
      newCount = checkForCompletion(list,expectedCount,0)
      if(newCount!=0 && newCount == preCount) do
        sameCount = sameCount+1
      else
        sameCount = 0
      end

      if(sameCount == 5) do
        true
      else
        :timer.sleep(100)
        checkForCompletion(list,expectedCount,newCount,sameCount)
      end
  end

  def checkForCompletion(list,expectedCount,count) do
    if(length(list) == 0) do
      count
    else
      [nid|list] = list
      source_node_name = "node_"<>nid
      state =  GenServer.call(String.to_atom(source_node_name),{:get_state,"state"})
      dest_list = elem(state,3)
     
      count = count + length(dest_list)
     
      checkForCompletion(list,expectedCount,count)
    end
  end

  def print_state(list,sum,count) do
    if(length(list) == 0) do
      IO.puts "Average count is : "
      avg = sum/count
      IO.puts avg
    else
      [nid|list] = list
      source_node_name = "node_"<>nid
      state =  GenServer.call(String.to_atom(source_node_name),{:get_state,"state"})
      dest_list = elem(state,3)
      sum = sum + sumList(dest_list,0)
      count = count + length(dest_list)
      print_state(list,sum,count)
    end
  end

  def sumList(list,sum) do
    if(length(list) == 0) do
      sum
    else
      [x|list] = list
      sum = sum + elem(x,1)
      sumList(list,sum)
    end
  end

  def callStartSearching(list) do
    if(length(list) == 0) do
    else
      [source_id|list] = list
      source_node_name = "node_"<>source_id
      GenServer.cast(String.to_atom(source_node_name),{:start_searching,"start"})
      callStartSearching(list)
    end
  end

  def getBitCount do
    bitCount = 16
    bitCount 
  end
  
  def parse_args(args) do
    {_, [input,noOfReq], _} = OptionParser.parse(args)
    {input_val,_} = Integer.parse(input)
    {reqCount,_} = Integer.parse(noOfReq)
    {input_val,reqCount}
  end
  
  def startNodes(list, immutableList, no_of_req) do
    if(length(list) == 0) do
    else
      [nodeid|list] = list
      node_name = "node_" <> nodeid
      
      GenServer.start_link(__MODULE__, {nodeid,[],0,[],no_of_req, immutableList}, name: String.to_atom(node_name))
      startNodes(list, immutableList, no_of_req)
    end
  end
   
  def init(data) do
      noOfFiles = elem(data,4)
      fileList = genFileList(noOfFiles,[])
      
      routingTable = generateRoutingTable(elem(data,5), elem(data,0))
      #IO.inspect routingTable
      state = {elem(data,0),routingTable,elem(data,2),elem(data,3),fileList,noOfFiles}
      {:ok, state}
  end
  
  def generateNodeId(n) do
    Integer.to_string(round(:math.pow(2,getBitCount)),2)
    
  end

  def getNodeList(n) do
    interval =  round(:math.floor(getNodeSpace/n))
    list = generateList(n,interval,0,[])
  end

  def getNodeSpace do
    :math.pow(2,getBitCount)
  end

  def getFileHash(filename) do
    String.slice(Base.encode16(:crypto.hash(:sha256, filename)),0,round(getBitCount/4))
  end

  def getRows(listOfNodes) do
    numNodes = length(listOfNodes)
    rows = Float.ceil(:math.log(numNodes)/:math.log(16))
  end

  def convertTo32bits(str) do
     getZeroes(getBitCount/4 - String.length(str),"") <> str
  end

  def generateRoutingTable(listOfNodes, nodeId) do
    rowsInRoutingTable = getRows(listOfNodes)
    routingTable = createRoutingTable(listOfNodes, nodeId, rowsInRoutingTable, 1, %{})
  end

  def getFileLocation(list, fileHash) do
     pos = 2
     listDest = getFileLocation(list,pos,fileHash,0)
     
     getNearestNode(listDest, fileHash,pos,18,fileHash)
  end

  def getRandomNeighbor(key, allPossibleNeighbors) do
    values = Map.get(allPossibleNeighbors,key)
    len = String.length(key)
    list = ["0","1","2","3","4","5","6","7","8","9","A","B","C","D","E","F"]
    values = Enum.shuffle(values)
    final = iter(values,[],list,len)
  end

  def getZeroes(n,str) do
     if(n>0) do
       str = getZeroes(n-1,str <> "0")
     end
     str
  end
  def getFileLocation(list,cnt,fileHash,position) do
    if(cnt == 0) do
      list
    else
      newList = getNewList(list,String.slice(fileHash,position..position),[],position)
      list = getFileLocation(newList,cnt-1,fileHash,position+1)
    end
    list
  end

  def getNewList(list,str,newList,cnt) do
    if(length(list) == 0) do
     newList
    else
     [x|list] = list
     if(String.slice(x,cnt..cnt) == str) do
       newList = [x|newList]
     end
     getNewList(list,str,newList,cnt)
    end
  end
 
  def createRoutingTable(listOfNodes, nodeId, rowsInRoutingTable, currRow , routingTable) do
    if(currRow <= rowsInRoutingTable) do
      prefix = String.slice(nodeId, 0..(currRow-1))
      allPossibleNeighbors = getNeighbors(prefix, listOfNodes, %{})
      keys = Map.keys(allPossibleNeighbors)
      routingTable = fillRoutingTable(prefix, keys, allPossibleNeighbors, routingTable)
      currRow = currRow + 1
      routingTable = createRoutingTable(listOfNodes, nodeId, rowsInRoutingTable, currRow , routingTable)
    end
    routingTable
  end

  def getNearestNode(list, fileHash, position, min, sofar) do
    if(list == nil || length(list) == 0) do
      sofar    
    else
      [firstElement | list] = list   
      dist =  String.to_integer(String.slice(fileHash,position..position),16) - String.to_integer(String.slice(firstElement,position..position),16)
      
      if(dist >=0 && dist<min) do
        sofar = firstElement
        min = dist
      end
      getNearestNode(list,fileHash,position,min,sofar)
    end
  end

  def fillRoutingTable(prefix, keys, allPossibleNeighbors, routingTable) do
    if(length(keys) > 0) do
      [key|keys] = keys
      neighbor = getRandomNeighbor(key, allPossibleNeighbors)
      alreadyNeighbor = Map.get(routingTable,prefix)
      if alreadyNeighbor == nil do
        alreadyNeighbor = []
      end
      if(String.length(prefix) - 1 == 0) do
        pre = ""
      else
        pre = String.slice(prefix,0..(String.length(prefix)-2))
      end 

      routingTable = Map.put(routingTable,pre , neighbor)
    end
    routingTable
  end

  

  def iter(values,final,list,len) do
    if(length(values) > 0) do
      [value|values] = values
      if updatelist(value, list,len) do
        final = [value|final]
        list = List.delete(list, String.slice(value, len-1..len-1))
      end
      final = iter(values,final,list,len)
    end
    final
  end

  def updatelist(value, list,len) do
    isMember = Enum.member?(list, String.slice(value, len-1..len-1))
  end

  def getNeighbors(prefix, listOfNodes, allPossibleNeighbors) do
    if(length(listOfNodes) != 0) do
      [a|b] = listOfNodes
      listOfNodes = b
      len = String.length(prefix)
      lenNode = String.length(a)

      currNodePrefix = String.slice(a, len-1..len-1)
      comparePrefix = String.slice(prefix, len-1..len-1)
      
      parentPossibleNeighborPrefix = ""
      parentPrefix = ""

      if(len > 1) do
        parentPossibleNeighborPrefix = String.slice(a, 0..len-2)
        parentPrefix = String.slice(prefix, 0..len-2)
      end

      if(parentPrefix == parentPossibleNeighborPrefix) do
        if(currNodePrefix != comparePrefix) do
          currNodeAllNeighbors = Map.get(allPossibleNeighbors, prefix)
          if(currNodeAllNeighbors == nil) do
            currNodeAllNeighbors = []
          end

          currNodeAllNeighbors = [a|currNodeAllNeighbors]
          allPossibleNeighbors = Map.put(allPossibleNeighbors, prefix, currNodeAllNeighbors)
        end
      end
      allPossibleNeighbors = getNeighbors(prefix, listOfNodes, allPossibleNeighbors)
    end
    allPossibleNeighbors
  end

  def genFileList(num,fileList) do
    fileHash = getFileHash(:crypto.strong_rand_bytes(30) |> Base.url_encode64)
    if(num > 0) do
       genFileList(num-1,[fileHash|fileList])
    else
      fileList
    end
  end

  def start_searching(nodeid,fileList,routingTable) do
     if(length(fileList) == 0) do
     else
      [fileHash|fileList] = fileList
      prefixLength = 0
      neighbour = findClosestNeighbor(fileHash,nodeid,routingTable,0)
      neighbourId = elem(neighbour,0)
      send(neighbourId,fileHash,prefixLength+1,nodeid,1)
      start_searching(nodeid,fileList,routingTable)
     end

  end
  def send(neighbourId,fileHash,prefixLength,node_requesting,hopCountSoFar) do
      neighbour_node_name = "node_"<>neighbourId
      spawn fn -> GenServer.call(String.to_atom(neighbour_node_name),{:receive_msg,{fileHash,prefixLength,node_requesting,hopCountSoFar}}) end
   
  end

  def send_dest_found(source_node_id,dest_node_id,hopcount) do
    source_node_name = "node_" <> source_node_id
    spawn fn -> GenServer.call(String.to_atom(source_node_name),{:destination_found,{dest_node_id,hopcount}}) end 
  end
 
  def handle_cast({:start_searching ,new_message},state) do
    nodeid = elem(state,0)
    fileList = elem(state,4)
    routingTable = elem(state,1)
    start_searching(nodeid,fileList,routingTable)
    {:noreply,state}
  end

  def handle_call({:receive_msg ,new_message}, _from,state) do
    
    fileHash = elem(new_message,0)
    prefixLength = elem(new_message,1)
    node_requesting = elem(new_message,2)
    hopCountSoFar = elem(new_message,3)
    neighbour = findClosestNeighbor(fileHash,elem(state,0),elem(state,1),prefixLength)
    neighbourId = elem(neighbour,0)
    if(elem(neighbour,1) == true) do
      send_dest_found(node_requesting,elem(state,0),hopCountSoFar)

    else
      send(neighbourId,fileHash,prefixLength+1,node_requesting,hopCountSoFar+1)
    end
    {:reply, state, state}   
  end

  def find_max_neighbor(fileHash, neighbors, currMaxLength, currMaxNeighbor) do
    if(length(neighbors) > 0) do
      [neighbor|neighbors] = neighbors
      currTuple = lcs(fileHash, neighbor)
      newLength = elem(currTuple,0)
      
      if currMaxLength < newLength do
        currMaxLength = newLength
        currMaxNeighbor = elem(currTuple,1)
      end
      
      currMaxNeighbor = find_max_neighbor(fileHash,neighbors,currMaxLength,currMaxNeighbor)
    end
    currMaxNeighbor
  end

  def findClosestNeighbor(file, startNode, routingTable, prefixLength) do
    currPrefix = ""
    filePrefix = ""
    if(prefixLength > 0) do
      currPrefix= String.slice(startNode, 0..prefixLength)
      filePrefix = String.slice(file, 0..prefixLength)
    end
    isLastHop = false
    nextHop = ""
      
      neighbors = Map.get(routingTable, currPrefix)
      
      allKeys = Map.keys(routingTable)      
      if neighbors == nil do
       
        nextHop = getNearestNode(Map.get(routingTable,String.slice(startNode, 0..prefixLength-1)), file, prefixLength, 18, file)
        isLastHop = true
      else 
        if !Enum.member?(neighbors, file) do
          maxPrefix = find_max_neighbor(file, allKeys , 0,"")
          newPrefix =  String.slice(file, 0..prefixLength)
          
          if(String.length(maxPrefix) != 0) do
            neighbors = Map.get(routingTable, maxPrefix)
            prefixLength = String.length(maxPrefix)
            newPrefix = String.slice(file, 0..prefixLength)
          end

          nextHoplist = Enum.filter(neighbors, fn(neighbor) ->  String.slice(neighbor, 0..prefixLength) == newPrefix end)
          
         
          if length(nextHoplist) != 0 do
            nextHop = Enum.random(nextHoplist)
          else
            nextHop = getNearestNode(neighbors, file, prefixLength, 18, file)
            isLastHop = true
          end
        end
      end
    {nextHop, isLastHop}
  end

  def lcs(fileHash, possibleHopNode) do
    i = 0
    max_length = checkEqual(fileHash, possibleHopNode, i)
    {max_length, possibleHopNode}
  end

  def checkEqual(fileHash, possibleHopNode, i) do
    if(String.slice(fileHash, i..i) == String.slice(possibleHopNode, i..i)) do
      i = i+1
      checkEqual(fileHash, possibleHopNode, i)
    else
      i
    end
  end

  def handle_call({:get_state ,new_message},_from,state) do  
    {:reply,state,state}
  end

  def handle_call({:destination_found,dest_details}, _from,state) do

    hopCount  = elem(dest_details,1)
    destNode = elem(dest_details,0)
    member = {destNode, hopCount}
    lst = [member | elem(state,3)]
    new_state = {elem(state,0),elem(state,1),elem(state,2)+1,lst,elem(state,4),elem(state,5)}
    fileCount = elem(state,5)
    
    if(fileCount==length(lst)) do
    end
    
    {:reply, new_state, new_state}  
  end

  def generateList(n,interval,curid,nodeList) do
    cur = Integer.to_string(curid,16)
    cur = convertTo32bits(cur)
    nodeList = [cur | nodeList]
    if( n > 1 ) do
      interval = round(:math.floor((getNodeSpace - curid - 1)/(n-1)))
      nodeList = generateList(n-1,interval,curid+interval,nodeList)
    end
    nodeList 
  end

end
