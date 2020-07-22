val l=List(10,20,30,40)

l.foreach(x=>println(x))

val l1=l.map(x=>x+1)

l.foreach(x=>println(x))

l1.foreach(x=>println(x))


val l2=l.foldLeft(0)((x,y)=>x+y)